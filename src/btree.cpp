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
    /// variables used for scanning
    this -> scanExecuting = false;
    this -> highValInt = 0;
    this -> lowValInt = 0;
    this -> lowOp = GT;
    this -> highOp = LT;

    /// node and leaf occupancy
    leafOccupancy = INTARRAYLEAFSIZE;
    nodeOccupancy = INTARRAYNONLEAFSIZE;

    /// get buffer manager
    bufMgr = bufMgrIn;

    /// get and output the name of the index from the relationName passed in
    std::ostringstream index_string;
    index_string << relationName << '.' << attrByteOffset;
    outIndexName = index_string.str();

    /// try to access file and if it does not exist create a new one
    try {
        Page *pageHead;
        /// access file and create meta index
        file = new BlobFile(index_string.str(), false);

        headerPageNum = file -> getFirstPageNo();
        bufMgr -> readPage(file, headerPageNum, pageHead);

        IndexMetaInfo *index_meta = (IndexMetaInfo *) pageHead; /// create meta page and store root page
        rootPageNum = index_meta -> rootPageNo();

        // unpin the page
        bufMgr -> unPinPage(file, headerPageNum, false);

    }
    catch(FileNotFoundException err) { /// catch exception if file not found, make new file
        /// variables used in function
        Page *pageHead;
        RecordId r_id;
        std::string r;
        /// make new file and create a header and root page
        file = new BlobFile(index_string.str(), true);
        bufMgr -> allocPage(file, headerPageNum, pageHead);

        Page *pageRoot;
        bufMgr -> allocPage(file, rootPageNum, pageRoot);

        /// create leaf node with root info
        LeafNodeInt *rootNode = (LeafNodeInt *) pageRoot;
        rootNode -> rightSibPageNo = 0;

        /// now create meta index
        IndexMetaInfo *index_meta = (IndexMetaInfo *) pageHead;

        index_meta -> attrType = attrType; /// fill attribute details from what was passed in
        index_meta -> attrByteOffset = attrByteOffset;
        strcpy(index_meta -> relationName, relationName.c_str());

        firstPageNumber = rootPageNum;
        index_meta -> rootPageNo = rootPageNum;

        /// instantiate a filescan to insert into new file
        FileScan scan(relationName, bufMgr);

        /// scan file until reaching EOF
        try {
            while (true) {
                scan.scanNext(r_id);
                r = scan.getRecord();
                insertEntry(r.c_str() + attrByteOffset, r_id);
            }
        }
        catch (EndOfFileException err) { }

        /// unpin pages and flush the file
        bufMgr ->  unPinPage(file, headerPageNum, true);
        bufMgr ->  unPinPage(file, rootPageNum, true);
        bufMgr -> flushFile(file);
    }
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

    /// first check to make sure that the opParms are valid, throw exception if not
    if ((lowOpParm != GT && lowOpParm != GTE) || (highOpParm != LT && highOpParm != LTE)){
        throw BadOpcodesException();
    }

    /// end current scan if one is already in progress
    if (this -> scanExecuting) {
        this -> endScan();
    }

    /// now get values to be used in the scan
    this -> highOp = highOpParm; /// opcodes
    this -> lowOp = lowOpParm;

    this -> lowValInt = *((int*) lowValParm); /// low and high values
    this -> highValInt = *((int*) highValParm);

    /// check that the scan range is valid, throw exception if not

    if (this -> lowValInt > this -> highValInt) {
        // reset variables
        this -> highOp = (Operator) -1;
        this -> lowOp = (Operator) -1;
        this -> lowValInt = 0;
        this -> highValInt = 0;
        throw BadScanrangeException();
    }

    this -> scanExecuting = true;
    /// loop variables for scanning
    PageId currentPageNum = rootPageNum;
    PageId next;
    bool notLeaf = true;
    bool gotLowerVal = false;
    int index = nodeOccupancy;


    /// check if the root is a leaf node, otherwise scan until finding first
    /// non - leaf node
    if (firstPageNumber == rootPageNum) {
        bufMgr -> readPage(file, rootPageNum, currentPageData);
    }
    else {
        /// extra variables used in scanning
        NonLeafNodeInt *scanPageNonLeaf;

        /// root is a non-leaf node so make it one and find leaf node
        while (true) {
            bufMgr -> readPage(file, currentPageNum, currentPageData);
            scanPageNonLeaf = (NonLeafNodeInt *) currentPageData;

            /// when level is equal to 1, then next level will contain leaf nodes
            if (scanPageNonLeaf -> level == 1) {
                while (!(scanPageNonLeaf -> pageNoArray[index]) && (index > -1)) {
                    index -= 1;
                } while (scanPageNonLeaf -> keyArray[index - 1] >= lowValInt && (index >0)) {
                    index -= 1;
                }
                /// reset index variable
                index = nodeOccupancy;

                /// unpin current page and update page number
                next = scanPageNonLeaf -> pageNoArray[index];
                bufMgr -> unPinPage(file, currentPageNum, false);
                currentPageNum = next;
                bufMgr -> readPage(file, currentPageNum, currentPageData);

                /// final read to get to the page on the leaf level
                 bufMgr -> readPage(file, currentPageNum, currentPageData);
                break;
            }

            /// move to next level, unpin page, and update current page number
            /// to get to the next level must find index by setting index to total node occupancy
            /// then decrementing it for each empty page and every key that is greater than our low val
            while (!(scanPageNonLeaf -> pageNoArray[index]) && (index > -1)) {
                index -= 1;
            } while (scanPageNonLeaf -> keyArray[index - 1] >= lowValInt && (index >0)) {
                index -= 1;
            }
            /// reset index variable
            index = nodeOccupancy;

            /// unpin current page and update page number
            next = scanPageNonLeaf -> pageNoArray[index];
            bufMgr -> unPinPage(file, currentPageNum, false);
            currentPageNum = next;
        }
    }


    while (!gotLowerVal) {
        bool validNode = false;
        // change leaf node with current page data
        LeafNodeInt *nodeLeaf  = (LeafNodeInt *) currentPageData;

        ///check that the page is not null before searching nod
        if ( !(nodeLeaf -> ridArray[0].page_number) ) {
            bufMgr -> unPinPage(file, currentPageNum, false); /// unpin page and throw exception if so
            throw NoSuchKeyFoundException();
        }
        int keyIndex = 0;
        while (keyIndex < leafOccupancy) {
            /// get current leaf node and check if it is lesser than our value
            int currValue = nodeLeaf -> keyArray[keyIndex];

            if (lowOp == GT && highOp == LT) {
                validNode = (currValue > lowValInt && currValue < highValInt);
            }
            else if (lowOp == GT && highOp == LTE) {
                validNode = (currValue > lowValInt && currValue <= highValInt);
            }
            else if (lowOp == GTE && highOp == LT) {
                validNode = (currValue >= lowValInt && currValue < highValInt);
            }
            else if (lowOp == GTE && highOp == LTE) {
                validNode = (currValue >= lowValInt && currValue <= highValInt);
            }

            if (validNode) {
                scanExecuting = true;
                nextEntry = keyIndex;
                gotLowerVal = true;
                break;
            }

            /// if at the last index, leaf does not contain value we are looking for
            if (keyIndex == leafOccupancy - 1) {
                bufMgr -> unpinPage(file, currentPageNum, false)
                if (nodeLeaf -> rightSibPageNo) {
                    currentPageNum = nodeLeaf -> rightSibPageNo;
                    bufMgr -> readPage(file, currentPageNum, false);
                }
                else {
                    throw NoSuchKeyFoundException();
                }
            }
            keyIndex += 1;
        }

    }
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

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
void BTreeIndex::endScan() 
{
    /// if trying to end scan when scan has not been started, throw exception
    if (!(scanExecuting)) {
        throw ScanNotInitializedException();
    }
    else { /// reset scan variables and unpin page
        currentPageData = NUll;
        nextEntry = -1;
        scanExecuting = false;

        bufMgr -> unpinPage(file, currentPageNum, false);
        currentPageNum = -1;
    }
}

}
