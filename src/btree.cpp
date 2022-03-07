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


const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
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


const void BTreeIndex::insertLeaf(LeafNodeInt *leaf, RIDKeyPair<int> entry)
{
  // empty leaf page
  if (leaf->ridArray[0].page_number == 0)
  {
    leaf->keyArray[0] = entry.key;
    leaf->ridArray[0] = entry.rid;    
  }
  else
  {
    int i = leafOccupancy - 1;
    // find the end
    while(i >= 0 && (leaf->ridArray[i].page_number == 0))
    {
      i--;
    }
    // shift entry
    while(i >= 0 && (leaf->keyArray[i] > entry.key))
    {
      leaf->keyArray[i+1] = leaf->keyArray[i];
      leaf->ridArray[i+1] = leaf->ridArray[i];
      i--;
    }
    // insert entry
    leaf->keyArray[i+1] = entry.key;
    leaf->ridArray[i+1] = entry.rid;
  }
}

const void BTreeIndex::insertNonLeaf(NonLeafNodeInt *nonleaf, PageKeyPair<int> *entry)
{
  
  int i = nodeOccupancy;
  while(i >= 0 && (nonleaf->pageNoArray[i] == 0))
  {
    i--;
  }
  // shift
  while( i > 0 && (nonleaf->keyArray[i-1] > entry->key))
  {
    nonleaf->keyArray[i] = nonleaf->keyArray[i-1];
    nonleaf->pageNoArray[i+1] = nonleaf->pageNoArray[i];
    i--;
  }
  // insert
  nonleaf->keyArray[i] = entry->key;
  nonleaf->pageNoArray[i+1] = entry->pageNo;
}


// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void* lowValParm,
           const Operator lowOpParm,
           const void* highValParm,
           const Operator highOpParm)
{
  
  lowValInt = *((int *)lowValParm);
  highValInt = *((int *)highValParm);

  if(!((lowOpParm == GT or lowOpParm == GTE) and (highOpParm == LT or highOpParm == LTE)))
  {
    throw BadOpcodesException();
  }
  if(lowValInt > highValInt)
  {
    throw BadScanrangeException();
  }

  lowOp = lowOpParm;
  highOp = highOpParm;

  // Scan is already started
  if(scanExecuting)
  {
    endScan();
  }

  currentPageNum = rootPageNum;
  // Start scanning by reading rootpage into the buffer pool
  bufMgr->readPage(file, currentPageNum, currentPageData);

  // root is not a leaf
  if(initialRootPageNum != rootPageNum)
  {
    // Cast
    NonLeafNodeInt* currentNode = (NonLeafNodeInt *) currentPageData;
    bool foundLeaf = false;
    while(!foundLeaf)
    {
      // Cast page to node
      currentNode = (NonLeafNodeInt *) currentPageData;
      // Check if this is the level above the leaf, if yes, the next level is the leaf
      if(currentNode->level == 1)
      {
        foundLeaf = true;
      }

      // Find the leaf
      PageId nextPageNum;
      findNextNonLeafNode(currentNode, nextPageNum, lowValInt);
      // Unpin
      bufMgr->unPinPage(file, currentPageNum, false);
      currentPageNum = nextPageNum;
      // read the nextPage
      bufMgr->readPage(file, currentPageNum, currentPageData);
    }
  }
  // Now the curNode is leaf node try to find the smallest one that satisefy the OP
  bool found = false;
  while(!found){
    // Cast page to node
    LeafNodeInt* currentNode = (LeafNodeInt *) currentPageData;
    // Check if the whole page is null
    if(currentNode->ridArray[0].page_number == 0)
    {
      bufMgr->unPinPage(file, currentPageNum, false);
      throw NoSuchKeyFoundException();
    }
    // Search from the left leaf page to the right to find the fit
    bool nullVal = false;
    for(int i = 0; i < leafOccupancy and !nullVal; i++)
    {
      int key = currentNode->keyArray[i];
      // Check if the next one in the key is not inserted
      if(i < leafOccupancy - 1 and currentNode->ridArray[i + 1].page_number == 0)
      {
        nullVal = true;
      }
      
      if(checkKey(lowValInt, lowOp, highValInt, highOp, key))
      {
        // select
        nextEntry = i;
        found = true;
        scanExecuting = true;
        break;
      }
      else if((highOp == LT and key >= highValInt) or (highOp == LTE and key > highValInt))
      {
        bufMgr->unPinPage(file, currentPageNum, false);
        throw NoSuchKeyFoundException();
      }
      
      // Did not find any matching key in this leaf, go to next leaf
      if(i == leafOccupancy - 1 or nullVal){
        //unpin page
        bufMgr->unPinPage(file, currentPageNum, false);
        //did not find the matching one in the more right leaf
        if(currentNode->rightSibPageNo == 0)
        {
          throw NoSuchKeyFoundException();
        }
        currentPageNum = currentNode->rightSibPageNo;
        bufMgr->readPage(file, currentPageNum, currentPageData);
      }
    }
  }
}


// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------


const void BTreeIndex::scanNext(RecordId& outRid) 
{
  if(!scanExecuting)
  {
    throw ScanNotInitializedException();
  }
	// Cast page to node
	LeafNodeInt* currentNode = (LeafNodeInt *) currentPageData;
  if(currentNode->ridArray[nextEntry].page_number == 0 or nextEntry == leafOccupancy)
  {
    // Unpin page and read papge
    bufMgr->unPinPage(file, currentPageNum, false);
    // No more next leaf
    if(currentNode->rightSibPageNo == 0)
    {
      throw IndexScanCompletedException();
    }
    currentPageNum = currentNode->rightSibPageNo;
    bufMgr->readPage(file, currentPageNum, currentPageData);
    currentNode = (LeafNodeInt *) currentPageData;
    // Reset nextEntry
    nextEntry = 0;
  }
 
  // Check  if rid satisfy
  int key = currentNode->keyArray[nextEntry];
  if(checkKey(lowValInt, lowOp, highValInt, highOp, key))
  {
    outRid = currentNode->ridArray[nextEntry];
    // Incrment nextEntry
    nextEntry++;
    // If current page has been scanned to its entirety
  }
  else
  {
    throw IndexScanCompletedException();
  }
}


// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//

const void BTreeIndex::endScan() 
{
  if(!scanExecuting)
  {
    throw ScanNotInitializedException();
  }
  scanExecuting = false;
  // Unpin page
  bufMgr->unPinPage(file, currentPageNum, false);
  // Reset variable
  currentPageData = nullptr;
  currentPageNum = static_cast<PageId>(-1);
  nextEntry = -1;
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
