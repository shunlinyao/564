/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"
#include <iostream>

using namespace std;

namespace badgerdb {

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++)
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

  int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}


BufMgr::~BufMgr() {
}

void BufMgr::advanceClock()
{
    clockHand = (clockHand+1) % numBufs;
}

void BufMgr::allocBuf(FrameId & frame)
{
    bool found = false;
    while (!found){
        advanceClock();
        BufDesc curr = bufDescTable[clockHand];
        if (curr.valid){
            if (curr.refbit){
                curr.refbit=false;
                continue;
            }
            else{
                if (curr.pinCnt == 0){
                    if (curr.dirty) {
                        flushFile(curr.file);
                    }
                    bufDescTable->Set(bufDescTable->file, curr.pageNo);
                }
                else {
                    continue;
                }
            }
        }
        else{
            bufDescTable->Set(bufDescTable->file, curr.pageNo);
        }
        frame = curr.frameNo;
        found = true;
    }
    cout << "allocBuf done!";
}


void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty)
{
    FrameId fid;
    try {
        hashTable->lookup(file, pageNo, fid);

        BufDesc frame = bufDescTable[fid];

        if (dirty)
            frame.dirty = true;

        if (frame.pinCnt == 0)
            throw PageNotPinnedException(file->filename(), pageNo, fid);
        else {
            frame.pinCnt--;
        }
    }
    catch (const HashNotFoundException &e) {
    }
    cout << "unPinPage done!";
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page)
{
    FrameId fid;
    Page temp = file->allocatePage();
    pageNo = temp.page_number();
    allocBuf(fid);
    hashTable->insert(file, pageNo, fid);
    bufDescTable->Set(file, pageNo);
    page = &temp;

    cout << "allocPage done!";
}

void BufMgr::flushFile(const File* file)
{
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{
}

void BufMgr::printSelf(void)
{
  BufDesc* tmpbuf;
	int validFrames = 0;

  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
