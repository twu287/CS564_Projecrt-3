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
	idxStr << relationName << '.' << attrByteOffset;
	std::string outIndexName = idxStr.str();

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
		file = new BlobFile(outIndexName, false );	
		headerPageNum = file->getFirstPageNo();
		Page *headPage;
		bufMgr->readPage(file, headerPageNum, headPage); //get the headerPAge
		IndexMetaInfo *metaInfo = (IndexMetaInfo*) headPage;
		rootPageNum = metaInfo->rootPageNo;

		if(metaInfo->attrByteOffset != attrByteOffset || metaInfo->attrType != attrType ||
			 metaInfo->relationName != relationName) 
			 {
				 throw BadIndexInfoException(outIndexName);
			 }
		bufMgr->unPinPage(file, headerPageNum, false); 
	}

	catch (FileNotFoundException e)
	{
		//create new file if file does not exist 
		file = new BlobFile(outIndexName, true );
		Page *headPage, *rootPage;
		bufMgr->allocPage(file, headerPageNum, headPage);
		bufMgr->allocPage(file, rootPageNum, rootPage);

		//initialize meta data
		IndexMetaInfo *metaInfo = (IndexMetaInfo*) headPage;
		metaInfo->attrByteOffset = attrByteOffset;
		metaInfo->attrType = attrType;
		metaInfo->rootPageNo = rootPageNum;
		memcpy(metaInfo->relationName, relationName.c_str(), relationName.size());
		metaInfo->relationName[relationName.size()] = '\0';

		//initialize rootpage
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
  bufMgr->flushFile(file);
  delete file;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

void BTreeIndex::scanNext(RecordId& outRid) 
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
void BTreeIndex::endScan() 
{

}

}
