#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#include <string.h>

#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "buffer_mgr_stat.h"


#define RC_FILE_EXISTED 5
#define RC_OPEN 6
#define RC_CLOSED 7
#define RC_FILE_DESTROY_FAILED 8
#define RC_PAGE_NOT_EXPECTED 9
#define RC_READ_FAILED 10
#define RC_FIX_NON_ZERO 11
#define RC_NOT_IN_POOL 12
// convenience macros
#define MAKE_MGMT()				\
((BM_Mgmt *)malloc(sizeof(BM_Mgmt)))
#define MAKE_CONTENT()				\
(malloc(sizeof(int)*bm->numPages))
#define MAKE_FLAG()				\
((bool *)malloc(sizeof(bool)*bm->numPages))

typedef struct BM_Mgmt {
    BM_PageHandle *ph;
    int seqNum;//tool for strategy FIFO and LRU
    bool dirty;//mark dirty or not
    int fix;//fix count
    int read;//readIO
    int write;//writeIO
    
} BM_Mgmt;//struct mgmt for


// Buffer Manager Interface Pool Handling
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
                  const int numPages, ReplacementStrategy strategy,
                  void *stratData){
    
    if(createPageFile((char*)pageFileName)!=RC_FILE_EXISTED){
        destroyPageFile((char*)pageFileName);
        return RC_FILE_NOT_FOUND;
    }//if file not existed, return error

    
    struct BM_Mgmt *mg=(BM_Mgmt *)malloc(sizeof(BM_Mgmt)*numPages);
    int i=0;
    for (i=0;i<numPages; i++) {
        mg[i].ph=NULL;
        mg[i].seqNum=-1;
        mg[i].dirty=false;
        mg[i].fix=0;
        mg[i].read=0;
        mg[i].write=0;
    }//initialize mgmt
    
    bm->pageFile=(char*)pageFileName;
    bm->numPages=numPages;
    bm->strategy=strategy;
    bm->mgmtData=mg;//initialize pool
    
    return RC_OK;
}

RC shutdownBufferPool(BM_BufferPool *const bm){
    
    struct BM_Mgmt *mg=bm->mgmtData;
    int i=0;
    for (i=0; i<bm->numPages; i++) {
        if(mg[i].fix!=0){
            return RC_FIX_NON_ZERO;
        }
    }//only all fix=0, it can be shutdown, or it return error
    
    forceFlushPool(bm);//call function
    
    for (i=0; i<bm->numPages; i++) {
        if (mg[i].ph!=NULL) {
            free(mg[i].ph);
            free(mg[i].ph->data);//free resources
        }
        mg[i].ph=NULL;
        mg[i].seqNum=-1;
        mg[i].dirty=false;
        mg[i].fix=0;
        mg[i].read=0;
        mg[i].write=0;
    }
    
    //free(bm->pageFile);
    bm->numPages=0;
    bm->strategy=0;

    return RC_OK;
}

RC forceFlushPool(BM_BufferPool *const bm){

    SM_FileHandle fh;
    openPageFile(bm->pageFile, &fh);
    
    struct BM_Mgmt *mg=bm->mgmtData;
    int i=0;
    for (i=0; i<bm->numPages; i++) {
        if(mg[i].dirty==true && mg[i].fix==0){//only if dirty=true and fix=0, it can be written back to disk
           writeBlock(mg[i].ph->pageNum, &fh, mg[i].ph->data);
           mg[i].dirty=false;
        }
    }
    
    closePageFile(&fh);
    return RC_OK;
}

// Buffer Manager Interface Access Pages
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page){
    struct BM_Mgmt *mg=bm->mgmtData;
    int i=0;
    for (i=0; i<bm->numPages; i++) {
        if (mg[i].ph!=NULL) {
            if (mg[i].ph->pageNum==page->pageNum) {
                memcpy(mg[i].ph->data,page->data,PAGE_SIZE);
                mg[i].dirty=true;//mark dirty
                break;
            }
        }
    }
    bm->mgmtData=mg;
    //printPageContent(page);
    return RC_OK;
}

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page){

    struct BM_Mgmt *mg=bm->mgmtData;
    int i=0;
    
    for (i=0; i<bm->numPages; i++){
        if (mg[i].ph!=NULL) {
            
            if (mg[i].ph->pageNum==page->pageNum) {
                //printf("%s",page->data);
                forcePage(bm, page);//check if need to write back to disk
                if (mg[i].fix!=0) {
                    mg[i].fix--;
                    break;
                }
            }
        }
    }

    bm->mgmtData=mg;
    
    
    return RC_OK;
}

RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page){
    struct BM_Mgmt *mg=bm->mgmtData;
    
    SM_FileHandle fh;
    openPageFile(bm->pageFile, &fh);

    int i=0;
    for (i=0; i<bm->numPages; i++) {
        if(mg[i].ph!=NULL){
            if (mg[i].ph->pageNum==page->pageNum){//make sure it is fit
                writeBlock(mg[i].ph->pageNum, &fh, page->data);
                if (mg[i].dirty) {
                    mg[i].write++;
                }
            }
            
            //printf("%s",page->data);
                
        }
    }
    
    bm->mgmtData=mg;
    closePageFile(&fh);

    return RC_OK;
}

RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page,
            const PageNumber pageNum){
    
    SM_FileHandle fh;
    openPageFile(bm->pageFile, &fh);
    //printf("%d",fh.totalNumPages);
    int num=bm->numPages;
    int pos=num;//pos to find null position
    int posMatch=num;//to find position with same page
    page->pageNum=pageNum;
    page->data=(SM_PageHandle)malloc(PAGE_SIZE);
    
    BM_PageHandle *newPage=MAKE_PAGE_HANDLE();
    newPage->data=(SM_PageHandle)malloc(PAGE_SIZE);
    
    if (fh.totalNumPages>pageNum) {
        readBlock(pageNum,&fh,page->data);
    }

    newPage->data=page->data;
    newPage->pageNum=page->pageNum;
    struct BM_Mgmt *mg=bm->mgmtData;
    int i=0;
    for (i=0; i<bm->numPages; i++) {
        if (mg[i].ph==NULL) {//check null position
            pos=i;
            break;
        }
        else {
            if (mg[i].ph->pageNum==pageNum) {//check same page
                posMatch=i;
            }
        }
        
    }
    if (pos!=num&&posMatch==num) {//if it has null position
        mg[pos].ph=newPage;
        mg[pos].fix++;
        mg[pos].read++;
        mg[pos].seqNum=pos;
    }
    else if (posMatch!=num){//if it has same page
        if (bm->strategy==RS_LRU) {
            
            
            for (i=0;i<bm->numPages;i++) {
                if (mg[i].seqNum>=mg[posMatch].seqNum) {
                    mg[i].seqNum--;
                }
            }
            mg[posMatch].seqNum=pos-1;
        }
        mg[posMatch].fix++;
    }
    else if (pos==num&&posMatch==num){//if it need replace page
        int min=INT_MAX;
        int poss=-1;
        for (i=0; i<bm->numPages; i++) {
            if (min>mg[i].seqNum&&mg[i].fix==0) {
                min=mg[i].seqNum;
                poss=i;
            }
        }
        mg[poss].ph=newPage;
        mg[poss].fix=1;
        mg[poss].read++;
        mg[poss].seqNum=num-1;
        

        //if (bm->strategy==RS_FIFO) {//strategy FIFO
            for (i=0; i<bm->numPages; i++) {
                if (mg[i].seqNum>min&&i!=poss) {
                    mg[i].seqNum=(mg[i].seqNum+num-1)%num;
                }
                
            }
        //}
       
        
    }
    closePageFile(&fh);
    bm->mgmtData=mg;
    //printPageContent(page);
    return RC_OK;
}

// Statistics Interface
PageNumber *getFrameContents (BM_BufferPool *const bm){
    int *result=MAKE_CONTENT();
    struct BM_Mgmt *mg=bm->mgmtData;
    int i=0;
    for (i=0; i<bm->numPages; i++) {
        if (mg[i].ph!=NULL) {
            result[i]=mg[i].ph->pageNum;//get pageNum
        }
        else{
            result[i]=-1;
        }
    }
    return result;
}

bool *getDirtyFlags (BM_BufferPool *const bm){
    bool *result=MAKE_FLAG();
    struct BM_Mgmt *mg=bm->mgmtData;
    int i=0;
    for (i=0; i<bm->numPages; i++) {
        result[i]=mg[i].dirty;//get dirty
    }
    return result;
}

int *getFixCounts (BM_BufferPool *const bm){
    int *result=MAKE_CONTENT();
    struct BM_Mgmt *mg=bm->mgmtData;
    int i=0;
    for (i=0; i<bm->numPages; i++) {
        result[i]=mg[i].fix;//get fix count
    }
    return result;
}
int getNumReadIO (BM_BufferPool *const bm){
    int sum=0;
    struct BM_Mgmt *mg=bm->mgmtData;
    int i=0;
    for (i=0; i<bm->numPages; i++) {
        sum=sum+mg[i].read;
    }
    return sum;
}
int getNumWriteIO (BM_BufferPool *const bm){
    int sum=0;
    struct BM_Mgmt *mg=bm->mgmtData;
    int i=0;
    for (i=0; i<bm->numPages; i++) {
        sum=sum+mg[i].write;
    }
    return sum;
}
