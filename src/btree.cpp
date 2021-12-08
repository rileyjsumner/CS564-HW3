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

}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
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
    if(nextEntry == -1) {
        throw new IndexScanCompletedException();
    }

    LeafNodeInt* leafNode = (LeafNodeInt*) currentPageData; // fetch current page data
    outRid = leafNode->ridArray[nextEntry];

    if(leafNode->keyArray[nextEntry + 1] == INT_MAX || nextEntry == leafOccupancy - 1) { // validate next entry
        if(leafNode->rightSibPageNo::null != NULL) {
            PageId new_pageID = leafNode->rightSibPageNo;
            Page* new_page;

            bufMgr->readPage(file, new_pageID, new_page);
            bufMgr->unPinPage(file, currentPageNum, false);

            currentPageData = new_page;
            currentPageNum = new_pageID;

            if((highOp == LTE && leafNode->keyArray[0] <= highValInt) || (highOp == LT && leafNode->keyArray[0] < highValInt)) {
                nextEntry = 0;
            } else {
                nextEntry = -1;
            }
        } else {
            nextEntry = -1;
        }
    } else {
        if((highOp == LTE && leafNode->keyArray[nextEntry+1] <= highValInt) || (highOp == LT && leafNode->keyArray[nextEntry+1] < highValInt)) {
            nextEntry++;
        } else {
            nextEntry = -1;
        }
    }
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
void BTreeIndex::endScan() 
{

}

}
