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

/**
 * Constructor of BTreeIndx
 * @param relationName    relation name
 * @param outIndexName    index name
 * @param bufMgrIn        buffer pool
 * @param attrByteOffset  off set of the attribute
 * @param attrType        attribute data type
 */
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

/**
 * destructor of BTreeIndx
 */
BTreeIndex::~BTreeIndex()
{
  bufMgr->flushFile(BTreeIndex::file);
  delete file;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

/**
 * function to inert a new entry with pair <key, rid>
 * @param key key to be inserted
 * @param rid rid to be inserted
 */
void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
  RIDKeyPair<int> dataEntry;
  dataEntry.set(rid, *((int *)key));
  Page* root;
  bufMgr->readPage(file, rootPageNum, root);
  PageKeyPair<int> *newEntry = nullptr;
  if (initialRootPageNum == rootPageNum) 
    insert(root, rootPageNum, true, dataEntry, newEntry);
  else
    insert(root, rootPageNum, false, dataEntry, newEntry);
}

/**
 * function to find the next level by the key 
 * @param curPage       current Page 
 * @param nextNodenum   the next level pageid
 * @param key           key used to check
*/
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

/**
 * function to insert index entry to index file
 * @param curPage     current page
 * @param curPageNum  current Page PageId
 * @param isLeafNode  check if current page is a leaf node
 * @param dataEntry   entry which needs to be inserted
 * @param newEntry    entry need to be moved up after splited, would be null if split is not necessary
*/
const void BTreeIndex::insert(Page *curPage, PageId curPageNum, bool isLeafNode, const RIDKeyPair<int> dataEntry, PageKeyPair<int> *&newEntry)
{
  if (!isLeafNode)
  {
    NonLeafNodeInt *curNode = (NonLeafNodeInt *)curPage;
    Page *nextPage;
    PageId nextNodeNum;
    findNextNonLeafNode(curNode, nextNodeNum, dataEntry.key);
    bufMgr->readPage(file, nextNodeNum, nextPage);
    isLeafNode = curNode->level == 1;
    insert(nextPage, nextNodeNum, isLeafNode, dataEntry, newEntry);
    
    if (newEntry == nullptr)
    {
      bufMgr->unPinPage(file, curPageNum, false);
    }
    else
    { 
      if (curNode->pageNoArray[nodeOccupancy] == 0)
      {
        // insert new entry to curpage
        insertNonLeafNode(curNode, newEntry);
        newEntry = nullptr;
        bufMgr->unPinPage(file, curPageNum, true);
      }
      else
      {
        splitNonLeafNode(curNode, curPageNum, newEntry);
      }
    }
  }
  else
  {
    LeafNodeInt *leaf = (LeafNodeInt *)curPage;
    // page is not full
    if (leaf->ridArray[leafOccupancy - 1].page_number == 0)
    {
      insertLeafNode(leaf, dataEntry);
      bufMgr->unPinPage(file, curPageNum, true);
      newEntry = nullptr;
    }
    else
    {
      splitLeafNode(leaf, curPageNum, newEntry, dataEntry);
    }
  }
}

/**
 * To insert an entry into a leaf
 * @param cur_leaf  leaf node that needs to be inserted into
 * @param entry     then entry needed to be inserted
 */
const void BTreeIndex::insertLeafNode(LeafNodeInt *cur_leaf, RIDKeyPair<int> entry) {
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
const void BTreeIndex::insertNonLeafNode(NonLeafNodeInt *cur_nonleaf, PageKeyPair<int> *entry) {
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

/**
 * function to insert a index entry which need to be splited
 * @param oldNode       the node which needs to be splited
 * @param oldPageNumer  odl PageId
 * @param newEntry      the new entry to add
*/
const void BTreeIndex::splitNonLeafNode(NonLeafNodeInt *oldNode, PageId oldPageNumber, PageKeyPair<int> *&newEntry)
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
    insertNonLeafNode(oldNode, newEntry);
  else
    insertNonLeafNode(newNode, newEntry);
  newEntry = &moveUpEntry;

  bufMgr->unPinPage(file, oldPageNumber, true);
  bufMgr->unPinPage(file, newPageNumber, true);

  // check if current node is root
  if (oldPageNumber == rootPageNum){
    updateRootNode(oldPageNumber, newEntry);
  }
}

/**
 * function to split leaf node when the inserted leafNode is full
 * @param leaf            leaf node which need to be splited
 * @param leafPageNumber  page number  of the leaf node
 * @param newEntry        data entry which need to move up
 * @param dataEntry       data entry which need to be inserted 
*/
const void BTreeIndex::splitLeafNode(LeafNodeInt *leaf, PageId leafPageNumber, PageKeyPair<int> *&newEntry, const RIDKeyPair<int> dataEntry)
{
  PageId newPageNumber;
  Page *newPage;
  // allocate a new node (leaf)
  bufMgr->allocPage(file, newPageNumber, newPage);
  LeafNodeInt *newLeafNode = (LeafNodeInt *)newPage;

  // split index
  int mid = leafOccupancy/2;
  // even keys scenario
  if (leafOccupancy %2 == 1 && dataEntry.key > leaf->keyArray[mid])
  {
    mid = mid + 1;
  }

  // move entries to the new node
  for(int i = mid; i < leafOccupancy; i++)
  {
    newLeafNode->keyArray[i-mid] = leaf->keyArray[i];
    newLeafNode->ridArray[i-mid] = leaf->ridArray[i];
    leaf->keyArray[i] = 0;
    leaf->ridArray[i].page_number = 0;
  }
  
  // check where to add the new entry
  if (dataEntry.key > leaf->keyArray[mid-1])
  {
    insertLeafNode(newLeafNode, dataEntry);
  }
  else
  {
    insertLeafNode(leaf, dataEntry);
  }

  // update sibling pointers
  newLeafNode->rightSibPageNo = leaf->rightSibPageNo;
  leaf->rightSibPageNo = newPageNumber;

  // the smallest key from second page as the new child entry
  newEntry = new PageKeyPair<int>();
  PageKeyPair<int> newKeyPair;
  newKeyPair.set(newPageNumber, newLeafNode->keyArray[0]);
  newEntry = &newKeyPair;
  
  bufMgr->unPinPage(file, leafPageNumber, true);
  bufMgr->unPinPage(file, newPageNumber, true);

  // if curr page is root
  if (leafPageNumber == rootPageNum)
  {
    updateRootNode(leafPageNumber, newEntry);
  }
}

/**
 * function to create a new root when root node is splited
 * @param firstPage the first page pageId in the root page
 * @param newcEntry the entry which need to move up
*/
const void BTreeIndex::updateRootNode(PageId firstPage, PageKeyPair<int> *newEntry)
{
  
  PageId newPageNumber;
  Page *newRoot;
  // allocate a new root node
  bufMgr->allocPage(file, newPageNumber, newRoot);
  NonLeafNodeInt *newRootPage = (NonLeafNodeInt *)newRoot;

  // update metadata
  if (initialRootPageNum == rootPageNum) 
    newRootPage->level = 1;
  else
    newRootPage->level = 0;
  newRootPage->pageNoArray[0] = firstPage;
  newRootPage->pageNoArray[1] = newEntry->pageNo;
  newRootPage->keyArray[0] = newEntry->key;

  Page *metaInfo;
  bufMgr->readPage(file, headerPageNum, metaInfo);
  IndexMetaInfo *metaPage = (IndexMetaInfo *)metaInfo;
  metaPage->rootPageNo = newPageNumber;
  rootPageNum = newPageNumber;
  
  bufMgr->unPinPage(file, headerPageNum, true);
  bufMgr->unPinPage(file, newPageNumber, true);
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

/**
 * function to start a sacn from root node to leaf node which contains the first record that satisfies the search criteria
 * @param lowVal  Low value of range
 * @param lowOp   Low operator(GT/GTE)
 * @param highVal High value of range
 * @param highOp  High operator(LT/LTE)
**/
void BTreeIndex::startScan(const void* lowValParm, const Operator lowOpParm, const void* highValParm, const Operator highOpParm)
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

/**
  * function to fetch the next record which is next to the entry that matches the scan criteria
  * @param outRid RecordId next to the record that satisfies the scan criteria
**/
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

/**
  * function to end the current scan
**/
void BTreeIndex::endScan() 
{
  if (!scanExecuting){
    throw ScanNotInitializedException();
  }
  bufMgr->unPinPage(file, currentPageNum, false);
  scanExecuting = false;
}

/**
  * function to check if the key matches search criteria
  * @param lowVal   Low value of range
  * @param lowOp    Low operator(GT/GTE)
  * @param highVal  High value of range
  * @param highOp   High operator(LT/LTE)
  * @param val      key value
  * @return         true if satisfies falses otherwise
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

