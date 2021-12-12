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
    this->scanExecuting = false;
    this->highValInt = 0;
    this->lowValInt = 0;
    this->lowOp = GT;
    this->highOp = LT;

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

        headerPageNum = file->getFirstPageNo();
        bufMgr->readPage(file, headerPageNum, pageHead);

        IndexMetaInfo *index_meta = (IndexMetaInfo *) pageHead; /// create meta page and store root page
        rootPageNum = index_meta->rootPageNo();

        // unpin the page
        bufMgr->unPinPage(file, headerPageNum, false);

    }
    catch(FileNotFoundException err) { /// catch exception if file not found, make new file
        /// variables used in function
        Page *pageHead;
        RecordId r_id;
        std::string r;
        /// make new file and create a header and root page
        file = new BlobFile(index_string.str(), true);
        bufMgr->allocPage(file, headerPageNum, pageHead);

        Page *pageRoot;
        bufMgr->allocPage(file, rootPageNum, pageRoot);

        /// create leaf node with root info
        LeafNodeInt *rootNode = (LeafNodeInt *) pageRoot;
        rootNode->rightSibPageNo = 0;

        /// now create meta index
        IndexMetaInfo *index_meta = (IndexMetaInfo *) pageHead;

        index_meta->attrType = attrType; /// fill attribute details from what was passed in
        index_meta->attrByteOffset = attrByteOffset;
        strcpy(index_meta->relationName, relationName.c_str());

        firstPageNumber = rootPageNum;
        index_meta->rootPageNo = rootPageNum;

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
        bufMgr-> unPinPage(file, headerPageNum, true);
        bufMgr-> unPinPage(file, rootPageNum, true);
        bufMgr->flushFile(file);
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

PageId initRootPageNo = this->index_meta->rootPageNo; // used later on to check if current root and initial root that we started with are the same
this->RootPageNum = this->index_meta->rootPageNo; // actual variable we use (from btree.h) for the root's page number

/**
 * This method inserts a new entry into the index using the pair <key, rid>
 *
 * @param key :  A pointer to the value (integer) we want to insert.
 * @param rid : The corresponding record id of the tuple in the base relation.
 */
void BTreeIndex::insertEntry(const void *key, const RecordId rid)
{
    RIDKeyPair<int> newPair;
    newPair.set(rid, *((int *)key)); // create new key-rid pair and set its values
    Page* root; // root of our tree
    bufMgr->readPage(this->file, this->RootPageNum, root);
    PageKeyPair<int> *newChild = null;
    // call insert helper method
    if (this->RootPageNum == this->initRootPageNo){
        insert(root, this->RootPageNum, newPair, newChild, true);
    } else {
        insert(root, this->RootPageNum, newPair, newChild, false);
    }
    
}

/**
 *  This is the main helper function for the insertEntry function above. We created this function because we want to be able to make
 *  recursive calls if necessary and make those calls easily manageable.
 *
 * @param currPage : the current page we're dealing with
 * @param currPageNo : the page number of the page in question
 * @param newPair : the RIDKeyPair corresponding to the new child we're inserting
 * @param newChild : the PageKeyPair corresponding to the new child we're inserting
 * @param isLeaf : boolean that specifies if we're inserting at a leaf or not
 */
void BTreeIndex::insert(Page *currPage, PageId currPageNo, const RIDKeyPair<int> newPair, PageKeyPair<int> *&newChild, bool isLeaf)
{
    // case when we're about to insert at a leaf
    if (isLeaf == true)
    {
      LeafNodeInt *leaf = (LeafNodeInt *)currPage;
      // if we have space at a certain existing leaf to insert the child, we do it straight away
      if (leaf->ridArray[this->leafOccupancy - 1].page_number == 0)
      {
        insertLeaf(leaf, newPair);
        bufMgr->unPinPage(this->file, currPageNo, true);
        newChild = null;
      } // otherwise, we create a new leaf before inserting the new child
      else
      {
          // step 1: create a new page and allocate it to buffer
          PageId newPageNum;
          Page *newPage;
          bufMgr->allocPage(this->file, newPageNum, newPage);
          LeafNodeInt *newLeafNode = (LeafNodeInt *)newPage;

          // step 2: find the point at which any shifts will be necessary. We start at the midpoint.
          int midpoint = this->leafOccupancy / 2;
          if (this->leafOccupancy % 2 == 1 && newPair.key > leaf->keyArray[midpoint])
          {
              midpoint++;
          }
          // step 3: we transfer half of the existing key and RID entries into newLeafNode by
          // copying them and then setting the original entries in the respective arrays to zero
          for(int i = midpoint; i < this->leafOccupancy; i++)
          {
            newLeafNode->keyArray[i-midpoint] = leaf->keyArray[i];
            newLeafNode->ridArray[i-midpoint] = leaf->ridArray[i];
            leaf->keyArray[i] = 0;
            leaf->ridArray[i].page_number = 0;
          }
          
          // step 4: perform actual inserting. If we have to insert beyond the midpoint..
          if (newPair.key > leaf->keyArray[midpoint - 1])
          {
              // pass newLeafNode
            insertLeaf(newLeafNode, newPair);
          }
          else
          {
              // otherwise, pass leaf
            insertLeaf(leaf, newPair);
          }

          // step 5: make necessary changes to sibling pointer upon insertion of new child
          newLeafNode->rightSibPageNo = leaf->rightSibPageNo;
          leaf->rightSibPageNo = newPageNum;

          // step 6: specify new child entry
          newChild = new PageKeyPair<int>();
          PageKeyPair<int> newKeyPair;
          newKeyPair.set(newPageNum, newLeafNode->keyArray[0]);
          newChild = &newKeyPair;
          
          // step 7: unpin pages in question
          bufMgr->unPinPage(this->file, currPageNo, true);
          bufMgr->unPinPage(this->file, newPageNum, true);

          // if the current page is the root, we make modifications to the root and the tree
          if (currPageNo == this->rootPageNum)
          {
              rootMods(currPageNo, newChild);
          }
      }
    }
    // case when we're about to insert anywhere but a leaf
    else
    {
    NonLeafNodeInt *currNode = (NonLeafNodeInt *)currPage;
    Page *nextPage;
    PageId nextNodeNo;
        
    // now, we search for the next non leaf node in the process of finding the right key
    int nextIdx = this->nodeOccupancy;
        
    for(int i = nextIdx; i >= 0 && (currNode->pageNoArray[i] == 0); i--)
    {
        nextIdx--;
    }
        
    for(int i = nextIdx; i > 0 && (currNode->keyArray[i-1] >= newPair.key); i--)
    {
        nextIdx--;
    }
        // assign the newly found index of the next non leaf node to the nextNodeNo variable
        nextNodeNo = currNode->pageNoArray[i];
    }
   this->bufMgr->readPage(this->file, nextNodeNo, nextPage);
    
    // change isLeaf according to changes in currNode's level
    if(currNode->level == 1) {
        isLeaf = true;
    } else {
        isLeaf = false;
    }
    // recursive call to insert function with updated values of the variables
    insert(nextPage, nextNodeNo, newPair, newChild, isLeaf);
    
    // if the child points to null and there is no split...
    if (newChild == null)
    {
        // ... we unpin the current page from the buffer
        this->bufMgr->unPinPage(this->file, currPageNo, false);
    } // split is needed
    else
      {
      // if there exists a free non leaf node...
      if (currNode->pageNoArray[this->nodeOccupancy] == 0)
      {
        // ...we insert the new child there and unpin the current page from the buffer
        insertNonLeaf(currNode, newChild);
        newChild = null;
        bufMgr->unPinPage(file, currPageNo, true);
      }
      // otherwise, we will have to create a new non leaf node
      else
      {
          PageId newPageNum;
          Page *newPage;
          bufMgr->allocPage(file, newPageNum, newPage);
          NonLeafNodeInt *newNode = (NonLeafNodeInt *)newPage;

          // here, we start to search for the right place to insert this new non leaf node
          int midpoint = this->nodeOccupancy / 2;
          int startIndex = midpoint; // default case is midpoint
          PageKeyPair<int> newPageKeyPair; // pair that we might have to add
          
          // if the number of nodes in the tree is even
          if (this->nodeOccupancy % 2 == 0)
          {
              // if the new child's key is less than the key of the midpoint...
              if (newChild->key < currNode->keyArray[midpoint]) {
                  // ... we start the insertion process at midpoint
                  startIndex = midpoint - 1;
              } else {
                  // otherwise we start right at the midpoint + 1
                  // (remember that indices start at 0 so we subtract above rather than adding here)
                  startIndex = midpoint;
              }
          }
          newPageKeyPair.set(newPageNum, currNode->keyArray[startIndex]);

          midpoint = startIndex + 1;
          
          // here, we start the transfer process by moving half of the entries towards the newly created node
          for(int i = midpoint; i < this->nodeOccupancy; i++)
          {
              newNode->keyArray[i - midpoint] = currNode->keyArray[i];
              newNode->pageNoArray[i - midpoint] = currNode->pageNoArray[i + 1];
              currNode->pageNoArray[i + 1] = (PageId) 0;
              currNode->keyArray[i + 1] = 0;
          }

          // copy current node data into new node and remove current node's records from the tree
          newNode->level = currNode->level;
          currNode->keyArray[startIndex] = 0;
          currNode->pageNoArray[startIndex] = (PageId) 0;
          
          // here, we do the actual insertion process of the new child into the newly created non leaf node
          if(newChild->key < newNode->keyArray[0]) {
              insertNonLeaf(currNode, newChild);
          } else {
              insertNonLeaf(newNode, newChild);
          }
          newChild = &newPageKeyPair;
          
          // unpin pages in question
          bufMgr->unPinPage(file, currPageNo, true);
          bufMgr->unPinPage(file, newPageNum, true);

          // if the current page is the root, we make modifications to the root and the tree
          if (currPageNo == this->rootPageNum)
          {
              rootMods(currPageNo, newChild);
          }
      }
    }
  }
}

/**
 * This is a secondary helper function that we created to modify the tree's data as we intend to inser a new root into the page.
 * In an instance as such, we'll hace to create a new root, update the metadata, and change the page number of the root
 * to know how to reference this new root.
 *
 * @param pageId : The page number of the root
 * @param newChild : PageKeyPair corresponding to the child we're trying to insert in the root
 */
void BTreeIndex::rootMods(PageId pageId, PageKeyPair<int> *newChild)
{
  // step 1: in order to split, first we create a new root
  PageId newRootNum;
  Page *newRoot;
  bufMgr->allocPage(file, newRootNum, newRoot);
  NonLeafNodeInt *pageNew = (NonLeafNodeInt *)newRoot;

  // step 2: as we have a new root, we need to update the metadata as necessary
    if(this->rootPageNum == this->initRootPageNo) {
        pageNew->level = 1;
    } else {
        pageNew->level = 0;
    }
    pageNew->keyArray[0] = newChild->key;
    pageNew->pageNoArray[0] = pageId;
    pageNew->pageNoArray[1] = newChild->pageNo;
  

  // step 3: getting the new meta info to change the rootPageNum and rootPageNo values in the metadata itself
  Page *meta;
  bufMgr->readPage(file, headerPageNum, meta);
  IndexMetaInfo *newMetaInfo = (IndexMetaInfo *)meta;
    
  // step 4: change the page number of the root in the metadata to that of the new root
  this->rootPageNum = newRootNum;
  newMetaInfo->rootPageNo = newRootNum;
    
  // step 5: unpin pages in question
  bufMgr->unPinPage(file, headerPageNum, true);
  bufMgr->unPinPage(file, newRootNum, true);
}

/**
 * This is a secondary helper function we created to perform the actual insertion process into the tree.
 * In this case, we're inserting into a leaf of the tree
 *
 * @param leaf : the pointer to the leaf node we're inserting the child to
 * @param newPair : The RIDKeyPair that corresponds to the new child we're about to insert
 */
void BTreeIndex::insertLeaf(LeafNodeInt *leaf, RIDKeyPair<int> newPair)
{
  // if empty just insert new child straight away
  if (leaf->ridArray[0].page_number == 0)
  {
    leaf->keyArray[0] = newPair.key;
    leaf->ridArray[0] = newPair.rid;
  }
    // otherwise, work our way towards...
  else
  {
    int endIdx = this->leafOccupancy - 1;
    // 1) finding the end
    for(int i = endIdx; i >= 0 && (leaf->ridArray[i].page_number == 0); i--) {
        endIdx--;
    }
    // 2) shifting the rest of the tree to make space for the new child
    for(int i = endIdx; i >= 0 && (leaf->keyArray[i] > newPair.key); i--) {
        leaf->keyArray[endIdx+1] = leaf->keyArray[endIdx];
        leaf->ridArray[endIdx+1] = leaf->ridArray[endIdx];
        endIdx--;
    }
    // 3) putting the new child in the tree
    leaf->keyArray[endIdx+1] = newPair.key;
    leaf->ridArray[endIdx+1] = newPair.rid;
  }
}

/**
 * This is a secondary helper function we created to perform the actual insertion process into the tree.
 * In this case, we're inserting into a non leaf of the tree
 *
 * @param nonLeaf : the pointer to the leaf node we're inserting the child to
 * @param currentChild : The RIDKeyPair that corresponds to the current child we're about to insert
 */
void BTreeIndex::insertNonLeaf(NonLeafNodeInt *nonLeaf, PageKeyPair<int> *currentChild)
{
  
  int endIdx = this->nodeOccupancy;
    
  // 1) finding the end
  for(int i = endIdx; i >= 0 && (nonLeaf->pageNoArray[i] == 0); i--)
  {
      endIdx--;
  }
    
  // 2) shifting the rest of the tree to make space for the new child
  for(int i = endIdx; i > 0 && (nonLeaf->keyArray[i-1] > currentChild->key); i--)
  {
      nonLeaf->keyArray[i] = nonLeaf->keyArray[i-1];
      nonLeaf->pageNoArray[i+1] = nonLeaf->pageNoArray[i];
      endIdx--;
  }
    
  // 3) putting the new child in the tree
  nonLeaf->keyArray[endIdx] = currentChild->key;
  nonLeaf->pageNoArray[endIdx+1] = currentChild->pageNo;
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
    if (this->scanExecuting) {
        this->endScan();
    }

    /// now get values to be used in the scan
    this->highOp = highOpParm; /// opcodes
    this->lowOp = lowOpParm;

    this->lowValInt = *((int*) lowValParm); /// low and high values
    this->highValInt = *((int*) highValParm);

    /// check that the scan range is valid, throw exception if not

    if (this->lowValInt > this->highValInt) {
        // reset variables
        this->highOp = (Operator) -1;
        this->lowOp = (Operator) -1;
        this->lowValInt = 0;
        this->highValInt = 0;
        throw BadScanrangeException();
    }

    this->scanExecuting = true;
    /// loop variables for scanning
    PageId currentPageNum = rootPageNum;
    PageId next;
    bool notLeaf = true;
    bool gotLowerVal = false;
    int index = nodeOccupancy;


    /// check if the root is a leaf node, otherwise scan until finding first
    /// non - leaf node
    if (firstPageNumber == rootPageNum) {
        bufMgr->readPage(file, rootPageNum, currentPageData);
    }
    else {
        /// extra variables used in scanning
        NonLeafNodeInt *scanPageNonLeaf;

        /// root is a non-leaf node so make it one and find leaf node
        while (true) {
            bufMgr->readPage(this->file, currentPageNum, currentPageData);
            scanPageNonLeaf = (NonLeafNodeInt *) currentPageData;

            /// when level is equal to 1, then next level will contain leaf nodes
            if (scanPageNonLeaf->level == 1) {
                while (!(scanPageNonLeaf->pageNoArray[index]) && (index > -1)) {
                    index -= 1;
                } while (scanPageNonLeaf->keyArray[index - 1] >= lowValInt && (index >0)) {
                    index -= 1;
                }
                /// reset index variable
                index = nodeOccupancy;

                /// unpin current page and update page number
                next = scanPageNonLeaf->pageNoArray[index];
                bufMgr->unPinPage(this->file, currentPageNum, false);
                currentPageNum = next;
                bufMgr->readPage(this->file, currentPageNum, currentPageData);

                /// final read to get to the page on the leaf level
                 bufMgr->readPage(this->file, currentPageNum, currentPageData);
                break;
            }

            /// move to next level, unpin page, and update current page number
            /// to get to the next level must find index by setting index to total node occupancy
            /// then decrementing it for each empty page and every key that is greater than our low val
            while (!(scanPageNonLeaf->pageNoArray[index]) && (index > -1)) {
                index -= 1;
            } while (scanPageNonLeaf->keyArray[index - 1] >= lowValInt && (index >0)) {
                index -= 1;
            }
            /// reset index variable
            index = nodeOccupancy;

            /// unpin current page and update page number
            next = scanPageNonLeaf->pageNoArray[index];
            bufMgr->unPinPage(this->file, currentPageNum, false);
            currentPageNum = next;
        }
    }


    while (!gotLowerVal) {
        bool validNode = false;
        // change leaf node with current page data
        LeafNodeInt *nodeLeaf  = (LeafNodeInt *) currentPageData;

        ///check that the page is not null before searching nod
        if ( !(nodeLeaf->ridArray[0].page_number) ) {
            bufMgr->unPinPage(this->file, currentPageNum, false); /// unpin page and throw exception if so
            throw NoSuchKeyFoundException();
        }
        int keyIndex = 0;
        while (keyIndex < leafOccupancy) {
            /// get current leaf node and check if it is lesser than our value
            int currValue = nodeLeaf->keyArray[keyIndex];

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
                bufMgr->unpinPage(this->file, currentPageNum, false)
                if (nodeLeaf->rightSibPageNo) {
                    currentPageNum = nodeLeaf->rightSibPageNo;
                    bufMgr->readPage(this->file, currentPageNum, currentPageData);
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

    /// fetch current node data
    LeafNodeInt* leafNode = (LeafNodeInt*) currentPageData;
    outRid = leafNode->ridArray[nextEntry];

    /// check if next entry is at the end of the node
    if(leafNode->keyArray[nextEntry + 1] == INT_MAX || nextEntry == leafOccupancy - 1) { // validate next entry
        /// has a right node been instantiated?
        if(leafNode->rightSibPageNo::null != NULL) {
            PageId new_pageID = leafNode->rightSibPageNo;
            Page* new_page;

            /// pin and unpin new and old pages
            bufMgr->readPage(this->file, new_pageID, new_page);
            bufMgr->unPinPage(this->file, currentPageNum, false);

            currentPageData = new_page;
            currentPageNum = new_pageID;

            /// Check if node should go back to start
            if((highOp == LTE && leafNode->keyArray[0] <= highValInt) || (highOp == LT && leafNode->keyArray[0] < highValInt)) {
                nextEntry = 0;
            } else {
                nextEntry = -1;
            }
        } else {
            nextEntry = -1;
        }
    } else {
        /// after handling special cases, check if scan should continue to next node
        if((this->highOp == LTE && leafNode->keyArray[nextEntry+1] <= highValInt) || (highOp == LT && leafNode->keyArray[nextEntry+1] < highValInt)) {
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

        bufMgr->unpinPage(this->file, currentPageNum, false);
        currentPageNum = -1;
    }
}

}
