#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "storage_mgr.h"
#include "dberror.h"



#define RC_FILE_EXISTED 5
#define RC_OPEN 6
#define RC_CLOSED 7
#define RC_FILE_DESTROY_FAILED 8
#define RC_PAGE_NOT_EXPECTED 9
#define RC_READ_FAILED 10
#define RC_NOT_EQUAL 11


/* manipulating page files */

void initStorageManager (void){
    printf("%s","welcome to the storage manager");//info to show its initializing
}

RC createPageFile (char *fileName){
    FILE *file;
    if(access(fileName,0)!=0){//check if target file is existing
        file=fopen(fileName,"w");//create target file
        char newPage[PAGE_SIZE];//allocate memory block of 4096 PAGE_SIZE
        memset(newPage,0,PAGE_SIZE);//fill block of allocated memory with 0
        if(fwrite(newPage,1,PAGE_SIZE,file)!=PAGE_SIZE){//write and check if write is failed
            return RC_WRITE_FAILED;//info to show if write failed
        }
    }
    else{
        return RC_FILE_EXISTED;//info to show if the file name was used
    }
    fclose(file);//close file
    return RC_OK;
}

RC openPageFile(char *fileName, SM_FileHandle *fHandle){
    
    if(access(fileName,0)==-1){
        return RC_FILE_NOT_FOUND;
    }//check if the file is existing
    FILE *file=fopen(fileName,"r");//open the file and read only
    
    fHandle->fileName=fileName;//assign the fileName
    
    fseek(file,0,SEEK_END);//put the position indicator to the end of file
    fHandle->totalNumPages=(int)ftell(file)/PAGE_SIZE;//get current value of position indicator and compute number of pages
    fHandle->curPagePos=0;//assign the current page position
    fHandle->mgmtInfo=(void*)RC_OPEN;//set the status of file, it must be open firstly if user wanna read or write
    
    fclose(file);//close
    return RC_OK;
}

RC closePageFile(SM_FileHandle *fHandle){
    if ((int)fHandle->mgmtInfo!=RC_OPEN) {
        return RC_FILE_HANDLE_NOT_INIT;
    }//make sure the file cannot be closed unless it is open
    fHandle->mgmtInfo=(void*)RC_CLOSED;
    return RC_OK;
}

RC destroyPageFile(char *fileName){
    
    if(access(fileName,0)==-1){
        return RC_FILE_NOT_FOUND;
    }//check if target is existing
    else if(remove(fileName)!=0){
        return RC_FILE_DESTROY_FAILED;
    }//destroy the file
    
    return RC_OK;
}

/* reading blocks from disc */

RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
    if((int)fHandle->mgmtInfo!=RC_OPEN){
        return RC_FILE_HANDLE_NOT_INIT;
    }//check if the file can be read
    
    if(access(fHandle->fileName,0)==-1){
        return RC_FILE_NOT_FOUND;
    }//check if the file is existing
    
    if(pageNum>=fHandle->totalNumPages||pageNum<0){
        return RC_READ_NON_EXISTING_PAGE;
    }//check if the target page is existing
    
    FILE *file=fopen(fHandle->fileName,"r");//open file and read only
    int pos=pageNum*PAGE_SIZE;//compute expected value of position indicator
    fseek(file, pos, SEEK_SET);//move position indicator to expected position
    if (ftell(file)==pos) {//check if indicator has been in expected position
        if(fread(memPage, 1, PAGE_SIZE, file)!=PAGE_SIZE){
            return RC_READ_FAILED;
        }//if so, read expected block from file
    }
    else{
        return RC_PAGE_NOT_EXPECTED;//if not, return error
    }
    fHandle->curPagePos=pageNum;//renew current page position
    fclose(file);//close
    return RC_OK;
}

int getBlockPos(SM_FileHandle *fHandle){
    return fHandle->curPagePos;//return current position
}

RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage){
    return readBlock(0, fHandle, memPage);//set pageNum=0 is the first block
}

RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage){
    return readBlock(fHandle->curPagePos-1, fHandle, memPage);//curPagePos-1 is the previous one
}

RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage){
    return readBlock(fHandle->curPagePos+1, fHandle, memPage);//curPagePos+1 is the next one
}

RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage){
    return readBlock(fHandle->totalNumPages-1, fHandle, memPage);//last block is totalNumPages-1
}

/* writing blocks to a page file */

RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
    if((int)fHandle->mgmtInfo!=RC_OPEN){
        return RC_FILE_HANDLE_NOT_INIT;
    }//check if the file can be read
    
    if(access(fHandle->fileName,0)==-1){
        return RC_FILE_NOT_FOUND;
    }//check if the file is existing
    
    if(pageNum>=fHandle->totalNumPages||pageNum<0){
        return RC_READ_NON_EXISTING_PAGE;
    }//check if the page is existing
    
    FILE *file=fopen(fHandle->fileName,"w");//open the file and it can be written
    int pos=pageNum*PAGE_SIZE;//compute expected value of position indicator
    fseek(file, pos, SEEK_SET);//move position indicator to expected position
    if (ftell(file)==pos) {//check if indicator has been in expected position
        if(fwrite(memPage,1,PAGE_SIZE,file)!=PAGE_SIZE){
            return RC_WRITE_FAILED;
        }//check if write failed
    }
    else{
        return RC_PAGE_NOT_EXPECTED;//if not, return error
    }
    
    fHandle->curPagePos=pageNum;//move position indicator into current page
    fclose(file);//close
    return RC_OK;
}

RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    return writeBlock(fHandle->curPagePos, fHandle, memPage);//pageNum=curPagePos
}

RC appendEmptyBlock (SM_FileHandle *fHandle){
    char newPage[PAGE_SIZE];//allocate memory block of 4096 PAGE_SIZE
    memset(newPage,0,PAGE_SIZE);//fill the block with 0
    int pageNum=fHandle->totalNumPages++;//make pageNum=totalNumPages then TotalNumPages=pageNum+1
    SM_PageHandle memPage=(SM_PageHandle)newPage;//convert newPage so that it can be used in function of writeBlock
    return writeBlock(pageNum, fHandle, memPage);//empty block is appended to last position
}
RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle){
    if((int)fHandle->mgmtInfo!=RC_OPEN){
        return RC_FILE_HANDLE_NOT_INIT;
    }//check if the file can be read
    
    if(access(fHandle->fileName,0)==-1){
        return RC_FILE_NOT_FOUND;
    }//check if the file is existing
    
    if(fHandle->totalNumPages!=numberOfPages){
        return RC_NOT_EQUAL;//ensure capacity
    }
    else{
        return RC_OK;
    }
}