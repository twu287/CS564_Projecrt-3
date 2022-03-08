/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"


//#define DEBUG

namespace badgerdb
{


// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------  


BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{
  
  //Construncting an index name
  std::ostringstream idxStr;
  idxStr << relationName << "." << attrByteOffset;
  outIndexName = idxStr.str();

	this->bufMgr = bufMgrIn;
  this->attrByteOffset = attrByteOffset;
  this->attributeType = attrType;
	this->attrByteOffset = attrByteOffset;
	this->bufMgr = bufMgrIn;
	leafOccupancy = INTARRAYLEAFSIZE;
	nodeOccupancy = INTARRAYNONLEAFSIZE;
	scanExecuting = false;

  try
  {
    //check if file exists
    file = new BlobFile(outIndexName, false);
    headerPageNum = file->getFirstPageNo();
    Page *headerPage;
    bufMgr->readPage(file, headerPageNum, headerPage);
    IndexMetaInfo *metaInfo = (IndexMetaInfo *)headerPage;
    rootPageNum = metaInfo->rootPageNo;

    if (relationName != metaInfo->relationName || attrType != metaInfo->attrType 
      || attrByteOffset != metaInfo->attrByteOffset)
    {
      throw BadIndexInfoException(outIndexName);
    }

    bufMgr->unPinPage(file, headerPageNum, false);    
  }
  //create new file if file does not exist 
  catch(FileNotFoundException e)
  {
    //File did not exist from upon, thus create a new blob file
    file = new BlobFile(outIndexName, true);
    Page *headerPage;
    Page *rootPage;
    bufMgr->allocPage(file, headerPageNum, headerPage);
    bufMgr->allocPage(file, rootPageNum, rootPage);

    //initialize meta data
    IndexMetaInfo *metaInfo = (IndexMetaInfo *)headerPage;
    metaInfo->attrByteOffset = attrByteOffset;
    metaInfo->attrType = attrType;
    metaInfo->rootPageNo = rootPageNum;
		memcpy(metaInfo->relationName, relationName.c_str(), relationName.size());
		metaInfo->relationName[relationName.size()] = '\0';
    
    // initiaize root
    initialRootPageNum = rootPageNum;
    LeafNodeInt *root = (LeafNodeInt *)rootPage;
    root->rightSibPageNo = 0;

    bufMgr->unPinPage(file, headerPageNum, true);
    bufMgr->unPinPage(file, rootPageNum, true);

		{
			//insert relation
			FileScan fscan(relationName, bufMgr);
			try
			{
				RecordId scanRid;
				while(1)
				{
					fscan.scanNext(scanRid);
					std::string recordStr = fscan.getRecord();
					void* key = (void*)(recordStr.c_str() + attrByteOffset);
					insertEntry(key, scanRid);
				}
			}
			catch(const EndOfFileException &e)
			{
				bufMgr->flushFile(file);
			}
		}
  }
}

// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------


BTreeIndex::~BTreeIndex()
{
  bufMgr->flushFile(BTreeIndex::file);
  delete file;
}


// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------


void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
  RIDKeyPair<int> dataEntry;
  dataEntry.set(rid, *((int *)key));
  Page* root;
  bufMgr->readPage(file, rootPageNum, root);
  PageKeyPair<int> *newchildEntry = nullptr;
  insert(root, rootPageNum, initialRootPageNum == rootPageNum ? true : false, dataEntry, newchildEntry);
}

const void BTreeIndex::findNextNonLeafNode(NonLeafNodeInt *curNode, PageId &nextNodeNum, int key)
{
  int i = nodeOccupancy;
  while(i >= 0 && (curNode->pageNoArray[i] == 0))
  {
    i--;
  }
  while(i > 0 && (curNode->keyArray[i-1] >= key))
  {
    i--;
  }
  nextNodeNum = curNode->pageNoArray[i];
}


const void BTreeIndex::insert(Page *curPage, PageId curPageNum, bool nodeIsLeaf, const RIDKeyPair<int> dataEntry, PageKeyPair<int> *&newchildEntry)
{
  if (!nodeIsLeaf)
  {
    NonLeafNodeInt *curNode = (NonLeafNodeInt *)curPage;
    Page *nextPage;
    PageId nextNodeNum;
    findNextNonLeafNode(curNode, nextNodeNum, dataEntry.key);
    bufMgr->readPage(file, nextNodeNum, nextPage);
    nodeIsLeaf = curNode->level == 1;
    insert(nextPage, nextNodeNum, nodeIsLeaf, dataEntry, newchildEntry);
    
    if (newchildEntry == nullptr)
    {
	    bufMgr->unPinPage(file, curPageNum, false);
    }
    else
	  { 
      if (curNode->pageNoArray[nodeOccupancy] == 0)
      {
        // insert the newchildEntry to curpage
        insertNonLeaf(curNode, newchildEntry);
        newchildEntry = nullptr;
        // finish the insert process, unpin current page
        bufMgr->unPinPage(file, curPageNum, true);
      }
      else
      {
        splitNonLeaf(curNode, curPageNum, newchildEntry);
      }
    }
  }
  else
  {
    LeafNodeInt *leaf = (LeafNodeInt *)curPage;
    // page is not full
    if (leaf->ridArray[leafOccupancy - 1].page_number == 0)
    {
      insertLeaf(leaf, dataEntry);
      bufMgr->unPinPage(file, curPageNum, true);
      newchildEntry = nullptr;
    }
    else
    {
      splitLeaf(leaf, curPageNum, newchildEntry, dataEntry);
    }
  }
}

/**
 * function to insert a index entry which need to do split
 * @param oldNode       the node which needs to be splited
 * @param oldPageNumer  odl PageId
 * @param newEntry      the new entry to add
*/
const void BTreeIndex::splitNonLeaf(NonLeafNodeInt *oldNode, PageId oldPageNumber, PageKeyPair<int> *&newEntry)
{
  
  Page *newPage;
  PageId newPageNumber;
  // allocate a new node (nonleaf)
  bufMgr->allocPage(file, newPageNumber, newPage);
  NonLeafNodeInt *newNode = (NonLeafNodeInt *)newPage;

  // split index
  int mid = nodeOccupancy/2;
  int moveUpIndex = mid;
  PageKeyPair<int> moveUpEntry;
  
  // even keys scenario
  if (nodeOccupancy % 2 == 0){
    moveUpIndex = newEntry->key < oldNode->keyArray[mid] ? mid -1 : mid;
  }

  moveUpEntry.set(newPageNumber, oldNode->keyArray[moveUpIndex]);

  mid = moveUpIndex + 1;

  // move entries to the new node
  for(int i = mid; i < nodeOccupancy; i++) {
    newNode->keyArray[i-mid] = oldNode->keyArray[i];
    newNode->pageNoArray[i-mid] = oldNode->pageNoArray[i+1];
    oldNode->pageNoArray[i+1] = (PageId) 0;
    oldNode->keyArray[i+1] = 0;
  }

  newNode->level = oldNode->level;
  // remove the entry that is pushed up from current node
  oldNode->keyArray[moveUpIndex] = 0;
  oldNode->pageNoArray[moveUpIndex] = (PageId) 0;

  // insert new entry
  if (newEntry->key < newNode->keyArray[0])
    insertNonLeaf(oldNode, newEntry);
  else
    insertNonLeaf(newNode, newEntry);
  newEntry = &moveUpEntry;

  bufMgr->unPinPage(file, oldPageNumber, true);
  bufMgr->unPinPage(file, newPageNumber, true);

  // check if current node is root
  if (oldPageNumber == rootPageNum){
    updateRoot(oldPageNumber, newEntry);
  }
}


/**
 * Helper function to splitLeafNode when the leafNode is full
 * @param leaf          Leaf node that is full
 * @param leafPageNum   The number of page of that leaf
 * @param newchildEntry The PageKeyPair that need to push up
 * @param dataEntry     The data entry that need to be inserted 
*/
const void BTreeIndex::splitLeaf(LeafNodeInt *leaf, PageId leafPageNum, PageKeyPair<int> *&newchildEntry, const RIDKeyPair<int> dataEntry)
{
  // allocate a new leaf page
  PageId newPageNum;
  Page *newPage;
  bufMgr->allocPage(file, newPageNum, newPage);
  LeafNodeInt *newLeafNode = (LeafNodeInt *)newPage;

  int mid = leafOccupancy/2;
  // odd number of keys
  if (leafOccupancy %2 == 1 && dataEntry.key > leaf->keyArray[mid])
  {
    mid = mid + 1;
  }
  // copy half the page to newLeafNode
  for(int i = mid; i < leafOccupancy; i++)
  {
    newLeafNode->keyArray[i-mid] = leaf->keyArray[i];
    newLeafNode->ridArray[i-mid] = leaf->ridArray[i];
    leaf->keyArray[i] = 0;
    leaf->ridArray[i].page_number = 0;
  }
  
  if (dataEntry.key > leaf->keyArray[mid-1])
  {
    insertLeaf(newLeafNode, dataEntry);
  }
  else
  {
    insertLeaf(leaf, dataEntry);
  }

  // update sibling pointer
  newLeafNode->rightSibPageNo = leaf->rightSibPageNo;
  leaf->rightSibPageNo = newPageNum;

  // the smallest key from second page as the new child entry
  newchildEntry = new PageKeyPair<int>();
  PageKeyPair<int> newKeyPair;
  newKeyPair.set(newPageNum, newLeafNode->keyArray[0]);
  newchildEntry = &newKeyPair;
  bufMgr->unPinPage(file, leafPageNum, true);
  bufMgr->unPinPage(file, newPageNum, true);

  // if curr page is root
  if (leafPageNum == rootPageNum)
  {
    updateRoot(leafPageNum, newchildEntry);
  }
}

/**
 * When the root needs to be split, create a new root node and insert the entry pushed up and update the header page 
 * @param firstPageInRoot   The pageId of the first pointer in the root page
 * @param newchildEntry     The keyPair that is pushed up after splitting
*/
const void BTreeIndex::updateRoot(PageId firstPageInRoot, PageKeyPair<int> *newchildEntry)
{
  // create a new root 
  PageId newRootPageNum;
  Page *newRoot;
  bufMgr->allocPage(file, newRootPageNum, newRoot);
  NonLeafNodeInt *newRootPage = (NonLeafNodeInt *)newRoot;

  // update metadata
  newRootPage->level = initialRootPageNum == rootPageNum ? 1 : 0;
  newRootPage->pageNoArray[0] = firstPageInRoot;
  newRootPage->pageNoArray[1] = newchildEntry->pageNo;
  newRootPage->keyArray[0] = newchildEntry->key;

  Page *metaInfo;
  bufMgr->readPage(file, headerPageNum, metaInfo);
  IndexMetaInfo *metaPage = (IndexMetaInfo *)metaInfo;
  metaPage->rootPageNo = newRootPageNum;
  rootPageNum = newRootPageNum;
  // unpin unused page
  bufMgr->unPinPage(file, headerPageNum, true);
  bufMgr->unPinPage(file, newRootPageNum, true);
}


/**
 * To insert an entry into a leaf
 * @param cur_leaf     leaf node that needs to be inserted into
 * @param entry        then entry needed to be inserted
 */
const void BTreeIndex::insertLeaf(LeafNodeInt *cur_leaf, RIDKeyPair<int> entry) {
	// it's empty
	if (cur_leaf->ridArray[0].page_number == 0) {
		cur_leaf->keyArray[0] = entry.key;
		cur_leaf->ridArray[0] = entry.rid;
	} else {
		int i = leafOccupancy - 1;
		while(i >= 0 && (cur_leaf->ridArray[i].page_number == 0)){
			i--;
		}
		for(;i >= 0 && (cur_leaf->keyArray[i] > entry.key);i--) {
			cur_leaf->keyArray[i+1] = cur_leaf->keyArray[i];
			cur_leaf->ridArray[i+1] = cur_leaf->ridArray[i];
		}
		cur_leaf->keyArray[i+1] = entry.key;
		cur_leaf->ridArray[i+1] = entry.rid;
	}
}

/**
 * To insert an entry into a non leaf
 * @param nonLeafNodeInt  nonleaf_getNext node that need to be inserted into
 * @param entry           then entry needed to be inserted
 *
 */
const void BTreeIndex::insertNonLeaf(NonLeafNodeInt *cur_nonleaf, PageKeyPair<int> *entry) {
	int i = nodeOccupancy;
	while(i >= 0 && (cur_nonleaf->pageNoArray[i] == 0)){
		i--;
	}
	for(;i > 0 && (cur_nonleaf->keyArray[i - 1] > entry->key); i--) {
		cur_nonleaf->keyArray[i] = cur_nonleaf->keyArray[i - 1];
		cur_nonleaf->pageNoArray[i + 1] = cur_nonleaf->pageNoArray[i];
	}

	cur_nonleaf->keyArray[i] = entry->key;
	cur_nonleaf->pageNoArray[i + 1] = entry->pageNo;
}	


// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

void BTreeIndex::startScan(const void* lowValParm,
           const Operator lowOpParm,
           const void* highValParm,
           const Operator highOpParm)
{
  
  if (scanExecuting){
		endScan();
	}
	if ((lowOpParm != GT && lowOpParm != GTE) || (highOpParm != LT && highOpParm != LTE)){
		throw BadOpcodesException();
	}
	lowValInt = *((int*)lowValParm);
	highValInt = *((int*)highValParm);
	lowOp = lowOpParm;
	highOp = highOpParm;
	if (lowValInt > highValInt){
		throw BadScanrangeException();
	}
	currentPageNum = rootPageNum;
	bufMgr->readPage(file, currentPageNum, currentPageData);

	//if root is not at leaf position
	if (initialRootPageNum != rootPageNum){
		NonLeafNodeInt* curPointer = (NonLeafNodeInt*) currentPageData;
		bool nextIsLeaf = false;
		while(!nextIsLeaf){
			curPointer = (NonLeafNodeInt*) currentPageData;
			if (curPointer->level == 1){
				nextIsLeaf = true;
			}
      PageId nextPageNum;
      findNextNonLeafNode(curPointer, nextPageNum, lowValInt);
			bufMgr->unPinPage(file, currentPageNum, false);
			//find nextpage at below level
			currentPageNum = nextPageNum;
			bufMgr->readPage(file, currentPageNum, currentPageData);
		}
	}

	bool foundSmallest = false;
	//last rid in this array (rid array is not full)
	bool noVal = false; 
	while(!foundSmallest){
		//這只有看小於lower bound的page是空的
		LeafNodeInt* curNode = (LeafNodeInt*) currentPageData;
		if (curNode->ridArray[0].page_number == 0){
			bufMgr->unPinPage(file, currentPageNum, false);
			throw NoSuchKeyFoundException();
		}
		for (int i = 0; i < leafOccupancy; i++){
			if (noVal){
				break;
			}
			//iterate to last element in rid array
			if (i < leafOccupancy - 1 && curNode->ridArray[i + 1].page_number == 0){
				noVal == true;
			}
			int keyValue = curNode->keyArray[i];
			if (checkKey(lowValInt, lowOp, highValInt,  highOp,  keyValue)){
				foundSmallest = true;
				nextEntry = i;
				scanExecuting = true;
				break;
			}
			else if ((highOpParm == LTE && !(keyValue <= highValInt))){
				bufMgr->unPinPage(file, currentPageNum, false);
				throw NoSuchKeyFoundException();
			}
			else if ((highOpParm == LT && !(keyValue < highValInt))){
				bufMgr->unPinPage(file, currentPageNum, false);
				throw NoSuchKeyFoundException();				
			}
      else {
			// keyValue < lowOpParm -> continue do for loop

				// (1) rid array is full, iterate all and no match 
				// (2) rid array is not full, iterate all and no match
				if (i == leafOccupancy - 1 || noVal){
					bufMgr->unPinPage(file, currentPageNum, false);
					int sibNo = curNode->rightSibPageNo;
					if (sibNo == 0){
						throw NoSuchKeyFoundException();
					}
					currentPageNum = sibNo;
					bufMgr->readPage(file, currentPageNum, currentPageData);
				}	
      }

		}

	}
}


// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------


void BTreeIndex::scanNext(RecordId& outRid) 
{
	if (!scanExecuting){
		throw ScanNotInitializedException();
	}
	LeafNodeInt* curNode = (LeafNodeInt*) currentPageData;
	if (nextEntry == leafOccupancy || curNode->ridArray[nextEntry].page_number == 0){
		bufMgr->unPinPage(file, currentPageNum, false);
		if (curNode->rightSibPageNo == 0){
			throw IndexScanCompletedException();
		}
		currentPageNum = curNode->rightSibPageNo;
		bufMgr->readPage(file, currentPageNum, currentPageData);
		curNode = (LeafNodeInt*) currentPageData;
		nextEntry = 0;
	}
	int keyValue = curNode->keyArray[nextEntry];
	if (checkKey(lowValInt, lowOp, highValInt,  highOp,  keyValue)){
		outRid = curNode->ridArray[nextEntry];
		nextEntry++;
	}
	else {
		throw IndexScanCompletedException();
	}
}


// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//

void BTreeIndex::endScan() 
{
	if (!scanExecuting){
		throw ScanNotInitializedException();
	}
	bufMgr->unPinPage(file, currentPageNum, false);
	scanExecuting = false;
}

/**
  * Helper function to check if the key is satisfies
  * @param lowVal   Low value of range, pointer to integer / double / char string
  * @param lowOp    Low operator (GT/GTE)
  * @param highVal  High value of range, pointer to integer / double / char string
  * @param highOp   High operator (LT/LTE)
  * @param val      Value of the key
  * @return True if satisfies False if not
  *
**/

const bool BTreeIndex::checkKey(int lowVal, const Operator lowOp, int highVal, const Operator highOp, int key)
{
  if(lowOp == GTE && highOp == LTE)
  {
    return key <= highVal && key >= lowVal;
  }
  else if(lowOp == GT && highOp == LTE)
  {
    return key <= highVal && key > lowVal;
  }
  else if(lowOp == GTE && highOp == LT)
  {
    return key < highVal && key >= lowVal;
  }
  else
  {
    return key < highVal && key > lowVal;
  }
}

}
