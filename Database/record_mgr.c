#include <stdlib.h>
#include <string.h>
#include "record_mgr.h"
#include "tables.h"
#include "storage_mgr.h"
#include "buffer_mgr.h"


typedef struct Record_mgmt {
	int num_record;//number of records in the table
	//BM_BufferPool *bm;
	int slot_size;//every slot size
        int record_size;//every record size
	int slotperpage;//slots per page
	char *table_name;//name of the table
        int s;   //sizeofSchema
	RID cur;				//points to the latest slot deleted
	RID pre;				//points to the second slot record deleted
	RID latest_slot;//the ID of the end of the table where can insert a slot as a new space
} RM;

typedef struct Scan_mgmt {
	RID cur_id;//current RID of the slot we need to start scan with
	Expr *cond;//condition which passed by the testcase with *sel,sel created by right,left
} sm;

//init number of records as num which is 0
int init_num_record(char *str, int pos, int num) {

	memcpy(str + pos, &num, sizeof(int));
	return pos + sizeof(int);//return the  offsite which we have written
}

//add RID as markers in the first page after num_of_Records
int add_flag(char *str, int position, RID *flag) {
	int pos = position;
	memcpy(str + pos, flag, sizeof(RID));
    //printf("add_:%d",flag->page);
    //  printf("add_:%d",flag->slot);
	return pos + sizeof(RID);
}

//get the RID_markers from the first page
int get_flag(char *str, int position, RID *flag) {

	int pos = position;
	memcpy(flag, str + pos, sizeof(RID));
	return pos + sizeof(RID);
}

//add the schema info to the first page with attNum[i](types,length,names,namelength)
int add_schema(char *str, int len, Schema *schema) {

    int pos = len;

    memcpy(str + pos, &(schema->numAttr), sizeof(int));
    pos += sizeof(int);

    memcpy(str + pos, &(schema->keySize), sizeof(int));
    pos += sizeof(int);

    int i;

    for (i = 0; i < schema->keySize; i++) {
        memcpy(str + pos, &(schema->keyAttrs[i]), sizeof(int));
        pos += sizeof(int);
    }

    for (i = 0; i < schema->numAttr; i++) {
        memcpy(str + pos, &(schema->dataTypes[i]), sizeof(int));
        pos += sizeof(int);

        memcpy(str + pos, &(schema->typeLength[i]), sizeof(int));
        pos += sizeof(int);

        int nameLength = strlen(schema->attrNames[i]);
        memcpy(str + pos, &(nameLength), sizeof(int));
        pos += sizeof(int);

        memcpy(str + pos, schema->attrNames[i], strlen(schema->attrNames[i]));
        pos += strlen(schema->attrNames[i]);

        //    printf("Name: %s\n", schema->attrNames[i]);
    }

    return pos;
}
//get the schema info to the first page with attNum[i](types,length,names,namelength)
int get_schema(char *str, int len, Schema **schema) {

    int numAttr = 0;
    int keySize = 0;
    int pos = len;

    memcpy(&numAttr, str + pos, sizeof(int));
    pos += sizeof(int);
    memcpy(&keySize, str + pos, sizeof(int));
    pos += sizeof(int);

    //   printf("get schema : total %d\n", numAttr);

    int *keys = (int *) malloc(sizeof(int) * keySize);
     
    DataType *dataType=(DataType*)malloc(sizeof(DataType)*numAttr);    
 
    int *typeLength = (int *) malloc(sizeof(int) * numAttr);

    char **attrNames = (char **) malloc(sizeof(char*) * numAttr);

    int i;
        for (i = 0; i < keySize; i++) {
        memcpy(keys + i, str + pos, sizeof(int));
        pos += sizeof(int);
    }

    int temp;
    int nameLength = 0;
    //  printf("NUMB ATTR %d\n",numAttr);
    for (i = 0; i < numAttr; i++) {
        memcpy(&temp, str + pos, sizeof(int));
        pos += sizeof(int);
        //printf("I v %d\n",pos);
        dataType[i] = (DataType)temp;

        //printf("get datatype : %d\n", dataType[i]);

        memcpy(typeLength + i , str + pos, sizeof(int));
        pos += sizeof(int);
               printf("I v %d\n",pos);
        //printf("get typelength : %d\n", *(typeLength +i));

        memcpy(&nameLength , str + pos, sizeof(int));
        pos += sizeof(int);
        //printf("I v %d\n",pos);
        //printf("Name length : %d\n", nameLength);

      // attrNames[i] = (char *) malloc(sizeof(char) * (nameLength+1));
	char *w=(char*)malloc(nameLength);
                  
	memcpy(w, str + pos ,nameLength);
        w[nameLength]='\0';
         attrNames[i]=w;       
        // int end=(int)strlen(w);
        // printf("sizeofW__ %d",end);
        // attrNames[i][end] = '\0';
        // attrNames[0]="a";
        // attrNames[1]="b";
        // attrNames[2]="c";
           pos =pos+1;
        // printf("I v %d\n",pos);
//         printf("get name : %s\n", attrNames[i]);
  //       printf("I v %d\n",pos); 
   }

    *schema = createSchema(numAttr, attrNames, dataType, typeLength, keySize, keys);
    return pos;

}

// get the first sizeof(int) bytes as the num of records
int get_total_number_records(char *str, int position, int *total_num_record) {
	int pos = position;
	memcpy(total_num_record,str + pos,  sizeof(int));
	return pos + sizeof(int);
}

RC initRecordManager(void *mgmtData) {
	initStorageManager();
	return RC_OK;
}


RC shutdownRecordManager() {
	printf("the RM has been shut down");
	return RC_OK;
}

RC createTable(char *name, Schema *schema) {
	SM_FileHandle fh;
	SM_PageHandle ph;
	int pos = 0;
	int init_num_records = 0;

	RID Pre;
	RID Cur;
	RID Latest;

	Pre.page = -1;
	Pre.slot = -1;

	Cur.page = -1;
	Cur.slot = -1;

	Latest.page = 1;
	Latest.slot = 1;

	createPageFile(name);
	openPageFile(name, &fh);
	ph = (char *) malloc(sizeof(char) * PAGE_SIZE);
	memset(ph, 0, sizeof(char) * PAGE_SIZE);

	pos = init_num_record(ph, pos, init_num_records);
	pos = add_schema(ph, pos, schema);
        int ss=pos-sizeof(int);
    //	pos = add_flag(ph, pos, Pre);
        memcpy(ph+pos,&Pre,sizeof(RID));
    //	pos = add_flag(ph, pos, Cur);
        memcpy(ph+pos+sizeof(RID),&Cur,sizeof(RID));
	//pos = add_flag(ph, pos, Latest);
        memcpy(ph+pos+2*sizeof(RID),&Latest,sizeof(RID));
    //printf("after create %d\n",pos);    //printf("%d",Pre->page);
    //printf("%d",Cur->page);
    //printf("%d",Latest->page);
	writeBlock(0, &fh, ph);
//	RID cdw;

	appendEmptyBlock(&fh);//data from 2nd page

	closePageFile(&fh);
	free(ph);

	return RC_OK;

}
RC openTable(RM_TableData *rel, char *name) {
	SM_FileHandle fh;
	SM_PageHandle ph;
	int pos = 0;
	int total_num_records;
	RID pre;
	RID cur;
	RID latest;
        int ss;
//	pre = (RID *) malloc(sizeof(RID));
//	cur = (RID *) malloc(sizeof(RID));
//	latest = (RID *) malloc(sizeof(RID));

	ph = (char *) malloc(sizeof(char) * PAGE_SIZE);
	memset(ph, 0, sizeof(char) * PAGE_SIZE);

	RM *m = (RM *) malloc(sizeof(RM));
	Schema *schema;

	openPageFile(name, &fh);

        readBlock(0,&fh,ph);

	pos = get_total_number_records(ph, pos, &total_num_records);

	pos = get_schema(ph, pos, &schema);
	ss=pos-sizeof(int);
	m->s=ss;
    /* for( i=0;i<schema->numAttr;i++){
     printf("type and length: %d ,%d",schema->dataTypes[i] ,schema->typeLength[i]);
     }*/
	m->table_name = (char *) malloc(sizeof(char) * strlen(name));
	strcpy(m->table_name, name);
	m->num_record = total_num_records;
	m->record_size = getRecordSize(schema);

    m->slot_size=m->record_size+2*sizeof(RID);// RID +DATA+ RID is a slot.
    //printf("slot_size:%d\n",m->slot_size);
    //  printf("%d",m->num_record);
     // printf("%d",m->slot_size);
	m->slotperpage = PAGE_SIZE/m->slot_size;
    // printf("np:%d",m->slotperpage);

       memcpy(&pre,ph+pos,sizeof(RID));	
       memcpy(&cur,ph+pos+sizeof(RID),sizeof(RID));
       memcpy(&latest,ph+pos+2*sizeof(RID),sizeof(RID));
    // printf("fuck open %d\n",pos);
       m->pre=pre;
       m->cur=cur;
       m->latest_slot=latest;

	rel->schema = schema;
	rel->name = name;
	rel->mgmtData = (void *) m;
        

	free(ph);
        closePageFile(&fh);
	return RC_OK;

}

//make sure data right 
RC closeTable(RM_TableData *rel) {
	SM_FileHandle fh;
	SM_PageHandle ph;
        ph = (char *) malloc(sizeof(char) * PAGE_SIZE);
	memset(ph, 0, sizeof(char) * PAGE_SIZE);

	RM *mgmt = (RM *) rel->mgmtData;

	openPageFile(mgmt->table_name, &fh);


//	RID cc;
        readBlock(0,&fh,ph);
    // printf("daxiao %d:",mgmt->s);
   // memcpy(&cc,ph+sizeof(int)+mgmt->s+2*sizeof(RID),sizeof(RID));
    //   printf("wozaizheli_close:%d\n",cc.page);
    //   printf("wozaizheli_close:%d\n",cc.slot);
    //	pos = add_int(ph, pos,m->num_record);

    //  printf("after close nr %d\n",pos);
    //	writeBlock(0, &fh, ph);
    //	closePageFile(&fh);
     // printf("Table has been closed");
    //
        free(ph);
    //  free(&fh);
	return RC_OK;
}

RC deleteTable(char *name) {
	destroyPageFile(name);
	printf("%s", name);
	return RC_OK;

}

int getNumTuples(RM_TableData *rel) {
	RM *m;
	m = (RM*) rel->mgmtData;
	return m->num_record;
}

//RID geter from the first Page
RID get_RID(int op, RM *mgmt) {
    RID temp;
	switch (op) {
        case 1:			//get pre RID
            temp=mgmt->cur;
            break;
        case 2:			//get next RID
            temp=mgmt->pre;
            break;
        case 3:			//get last RID
            temp=mgmt->latest_slot;
            break;
	}
    return temp;
}

// write_records handle to the RID we want
void write_records(SM_PageHandle ph, RM *mgmt, Record *record,RID ptr) {
    int pos=0;
    memcpy(ph + (record->id.slot - 1) * mgmt->slot_size,&(record->id),
           sizeof(RID));
    pos=(record->id.slot - 1) * mgmt->slot_size+sizeof(RID);
    //printf("pos: %d",pos);
    memcpy(ph + (record->id.slot - 1) * mgmt->slot_size+sizeof(RID),record->data,
           mgmt->record_size);
    memcpy(
           ph + record->id.slot * mgmt->slot_size - sizeof(RID),&ptr, sizeof(RID));

    //   writeBlock(record->id.page, &fh, ph);

}

// read_records_Data and ptr pointer
RID read_records(RID id, SM_FileHandle fh, Record *record, RM *mgmt,RID ptr) {
	SM_PageHandle ph;
	ph = (char *) malloc(sizeof(char) * PAGE_SIZE);
	memset(ph, 0, sizeof(char) * PAGE_SIZE);
	readBlock(id.page, &fh, ph);
        memcpy(&(record->id), ph + (id.slot - 1) * mgmt->slot_size,sizeof(RID));

        memcpy(record->data, ph + (id.slot - 1) * mgmt->slot_size+sizeof(RID),
           mgmt->record_size);
       	memcpy(&ptr, ph + id.slot * mgmt->slot_size- sizeof(RID),
        sizeof(RID));
	return ptr;
}

// handling records in a table
RC insertRecord(RM_TableData *rel, Record *record) {
	RM * mgmt = (RM *) rel->mgmtData;
	SM_FileHandle fh;

//char* dts=(char*)malloc(4);
//int a;
//int b;
//memcpy(&a,record->data,sizeof(int));
//memcpy(dts,record->data+4,4);
//memcpy(&b,record->data+8,4);
//printf("ChaRu_data %d____%s_____%d\n",a,dts,b);
	SM_PageHandle ph;
	//RH *rh;			//? space??
	RID now;
	RID new;
        RID ptr;
	//rh=(RH*)malloc(sizeof(RH));
	ptr.page = -1;
    // printf("Page_nu:%d\n",mgmt->latest_slot.page);
    // printf("Slot_nu:%d\n",mgmt->latest_slot.slot);
	ph = (char *) malloc(sizeof(char) * PAGE_SIZE);
	memset(ph, 0, sizeof(char) * PAGE_SIZE);
	openPageFile(mgmt->table_name, &fh);
       
        readBlock(0,&fh,ph);
//     printf("wocaonima222... %d\n",ph);
//printdata:    
    // printf("%d",mgmt->num_record);
    if (mgmt->cur.page == -1) {
        now.page = mgmt->latest_slot.page;
        now.slot = mgmt->latest_slot.slot;
        record->id=now;
	readBlock(now.page, &fh, ph);
        //write_records(ph, mgmt,record,ptr);
        //writeBlock(record->id.page, &fh, ph);
        memcpy(ph + (record->id.slot - 1) * mgmt->slot_size,&(record->id),
               sizeof(RID));
        memcpy(ph + (record->id.slot - 1) * mgmt->slot_size+sizeof(RID),record->data,
               mgmt->record_size);
	//printf("record size : %d", mgmt->record_size);

        memcpy(
               ph + record->id.slot * mgmt->slot_size - sizeof(RID),&ptr, sizeof(RID));

        //printf("!!!!!!!%d\n___ %d\n ",now.page, now.slot);
//test
       // char *xc=(char*)malloc(12);
       // int q=0;int y=0;
       // char *u=(char*)malloc(4);
       // memcpy(xc,ph+(record->id.slot-1)*mgmt->slot_size+sizeof(RID),sizeof(mgmt->record_size));
       // memcpy(&q,xc,4);
       // memcpy(u,xc+4,4);
       // memcpy(&y,xc+8,4);
       // printf("before write:>>>>>>>>%d____%s____%d\n",q,u,y);
        writeBlock(now.page, &fh, ph);
//	closePageFile(&fh);

//	openPageFile(mgmt->table_name, &fh);

//test //
       // char *t=(char*)malloc(12);
       // memset(ph, 0, PAGE_SIZE);
       // readBlock(now.page,&fh,ph);
//memcpy(t,ph+(now.slot-1)*mgmt->slot_size+sizeof(RID),sizeof(mgmt->record_size));
//memcpy(&a,t,4);
//memcpy(dts,t+4,4);
//memcpy(&b,t+8,4);
//printf("Duchu_data %d>>>>>%s>>>>>%d\n",a,dts,b);
       //new slot place handle
                if (now.slot == mgmt->slotperpage) {
			appendEmptyBlock(&fh);
			new.slot = 1;
			new.page = now.page + 1;
		}

		if (now.slot < mgmt->slotperpage) {
			new.slot = now.slot + 1;
			new.page = now.page;
		}
         writeBlock(now.page, &fh, ph);
	readFirstBlock(&fh, ph);
//Schema* wy;
//int ax=get_schema(ph,sizeof(int),&wy);
        mgmt->latest_slot=new;
        memcpy( ph + sizeof(int) + mgmt->s + 2*sizeof(RID),&new, sizeof(RID));
        writeBlock(0, &fh, ph);
      
        //printf("after:%d\n",mgmt->latest_slot.page);
        // 	 printf("after:%d\n",mgmt->latest_slot.slot);

	}
//if has empty slot place before the new place
	if (mgmt->cur.page >= 1) {
		now.page = mgmt->cur.page;
		now.slot = mgmt->cur.slot;
		record->id = now;
		readBlock(now.page, &fh, ph);

		if (mgmt->pre.page == -1) {
			mgmt->cur.page = -1;
			write_records(ph, mgmt,record,ptr);
			writeBlock(now.page, &fh, ph);

			readFirstBlock(&fh, ph);
			memcpy(ph + sizeof(int) + mgmt->s + sizeof(mgmt->pre),
                   &(mgmt->cur), sizeof(RID));
			writeBlock(0, &fh, ph);
		}

		if (mgmt->pre.page >= 1) {
			RID temp;
			temp = mgmt->pre;
			mgmt->cur = temp;
			mgmt->pre = read_records(mgmt->cur, fh, record,mgmt,ptr);
			ptr.page = -1;
			write_records(ph, mgmt, record,ptr);
			writeBlock(now.page, &fh, ph);

			readFirstBlock(&fh, ph);
			memcpy(ph + sizeof(int) +mgmt->s, &(mgmt->pre),
                   sizeof(RID));
			memcpy(ph + sizeof(int) + mgmt->s + sizeof(mgmt->pre),
                   &(mgmt->cur), sizeof(RID));
			writeBlock(0, &fh, ph);
		}
	}
    int temp=mgmt->num_record;
    temp=temp+1;
    mgmt->num_record=temp;
    //  printf("numb:%d",temp);
    readFirstBlock(&fh, ph);
    memcpy(ph,&temp,sizeof(int));
    writeBlock(0, &fh, ph);
    rel->mgmtData = (void *) mgmt;
    //free(ph);
    //closePageFile(&fh);
  //  int a;
   // readBlock(0,&fh,ph);
//Schema *schema2;
//int pos=get_schema(ph,4,&schema2);
   // memcpy(&a,ph,sizeof(int));
   // RID fuck;
   // memcpy(&fuck,ph+sizeof(int)+mgmt->s+2*sizeof(RID),sizeof(RID));
    return RC_OK;
}

// remove the record and set with 0
void remove_R(RM *mgmt, SM_PageHandle ph, RID id) {

	int slot = id.slot;
    //printf("delete_size %d",sizeof(char) * mgmt->slot_size - sizeof(RID));
	memset(ph + (slot - 1) * mgmt->slot_size, 0,
           sizeof(char) * mgmt->slot_size - sizeof(RID));

}

// set_ptr to link the empty slot_size place
void set_pre_ptr(RM *mgmt, SM_PageHandle ph, RID id, RID ptr) {
	RID pre_ptr = ptr;
	int slot = id.slot;
	memcpy(((
             ph + (slot - 1) * mgmt->slot_size
             + mgmt->slot_size) - sizeof(RID)), &pre_ptr,
           sizeof(RID));
}

// delete and make the ptr got its RID
RC deleteRecord(RM_TableData *rel, RID id) {
	RM * mgmt = (RM *) rel->mgmtData;

	SM_FileHandle fh;
	SM_PageHandle ph;
	RID now;

	now = id;
    //printf("opp:%d\n",now.page);
    //printf("ops:%d\n",now.slot);
	ph = (char *) malloc(sizeof(char) * PAGE_SIZE);
	memset(ph, 0, sizeof(char) * PAGE_SIZE);
	openPageFile(mgmt->table_name, &fh);

	if (mgmt->cur.page == -1) {
		readFirstBlock(&fh, ph);
		memcpy(ph + sizeof(int) + mgmt->s + sizeof(RID), &id,
                sizeof(RID));
		writeBlock(0, &fh, ph);

		readBlock(id.page, &fh, ph);

		remove_R(mgmt, ph, id);

		writeBlock(id.page, &fh, ph);
	}

	if (mgmt->cur.page >= 1) {
		RID temp;
		readFirstBlock(&fh, ph);
		temp = mgmt->cur;
		mgmt->pre = temp;
		mgmt->cur = id;
		memcpy(ph + sizeof(int) + mgmt->s, &mgmt->pre, sizeof(RID));
		memcpy(ph + sizeof(int) + mgmt->s + sizeof(mgmt->pre), &mgmt->cur,
                sizeof(RID));
		writeBlock(0, &fh, ph);

		readBlock(id.page, &fh, ph);
		mgmt->cur = id;

		remove_R(mgmt, ph, id);

		set_pre_ptr(mgmt, ph, id, temp);

		writeBlock(id.page, &fh, ph);
	}
    mgmt->cur=now;
    rel->mgmtData = (void *) mgmt;

	// printf("_________\n");
	// printf("latest.page:%d\n",mgmt->latest_slot.page);
	//	printf("latest.slot:%d\n",mgmt->latest_slot.slot);
	// printf("cur.page:%d\n",mgmt->cur.page);
    //		printf("cur.slot:%d\n",mgmt->cur.slot);
	//	printf("pre.page:%d\n",mgmt->pre.page);
	//	printf("pre.slot:%d\n",mgmt->pre.slot);
	//	printf("_________\n");

    RID cc;
    readBlock(0,&fh,ph);
    memcpy(&cc,ph+sizeof(int)+mgmt->s+2*sizeof(RID),sizeof(RID));
    // printf("wozaizheli:%d\n",cc.page);
    //  printf("wozaizheli:%d\n",cc.slot);

    closePageFile(&fh);
	return RC_OK;
}

//find by id and update the record->data
RC updateRecord(RM_TableData *rel, Record *record) {
	RM * mgmt = (RM *) rel->mgmtData;
	SM_FileHandle fh;
	SM_PageHandle ph;

	ph = (char *) malloc(sizeof(char) * PAGE_SIZE);
	memset(ph, 0, sizeof(char) * PAGE_SIZE);
	openPageFile(mgmt->table_name, &fh);

	RID now = record->id;

	readBlock(now.page, &fh, ph);

	memcpy(ph + (now.slot - 1) * mgmt->slot_size+sizeof(RID), record->data, mgmt->record_size);

	writeBlock(now.page, &fh, ph);
        rel->mgmtData=(void*)mgmt;


        closePageFile(&fh);
	return RC_OK;

}

RC getRecord(RM_TableData *rel, RID id, Record *record) {
	RM * mgmt = (RM *) rel->mgmtData;
	SM_FileHandle fh;
	SM_PageHandle ph;

	ph = (char *) malloc(sizeof(char) * PAGE_SIZE);
	memset(ph, 0, sizeof(char) * PAGE_SIZE);
	openPageFile(mgmt->table_name, &fh);

    char * d=(char*)malloc(mgmt->record_size);
    record->data=d;
    //printf("page: %d\n",id.page);
    // printf("slot: %d\n",id.slot);
	readBlock(id.page, &fh, ph);
	int slot = id.slot;
	int page = id.page;
    //printf("size: %d\n",mgmt->slot_size);
    //printf("size: %d\n",(slot-1)*mgmt->slot_size+sizeof(RID));
	//record->data=(char*)malloc(sizeof(mgmt->record_size)-2*sizeof(RID));
    //    printf ("size: %d\n",(slot-1)*mgmt->slot_size+sizeof(RID));
    memcpy(record->data, ph +(slot-1)*mgmt->slot_size+sizeof(RID),
         mgmt->record_size);
	record->id.page = page;
	record->id.slot = slot;
    int a;char *c =(char*)malloc(4);
    Value *Attr_Data;
    memcpy(&a,record->data,sizeof(int));
    memcpy(c,record->data+4,sizeof(4));
    MAKE_STRING_VALUE(Attr_Data, c);
    MAKE_VALUE(Attr_Data, DT_INT, a);
   // printf("----get_r_data_int1:%d\n",a);
   // printf("get_r_data_String:%s\n",c);
    memcpy(&a,record->data+8, sizeof(int) );
   // printf("get_r_data_int2:%d\n",a);
    rel->mgmtData=(void*)mgmt;
    free(ph);
    closePageFile(&fh);
    return RC_OK;
}

// scan init *cond is created by right and left exp
RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond) {

	sm *s_mgmt = (sm *) malloc(sizeof(sm));
        RM * r_mgmt=(RM*)malloc(sizeof(RM));
	s_mgmt->cond = cond;
	s_mgmt->cur_id.page  = 1;
	s_mgmt->cur_id.slot = 1;

	scan->rel = rel;
	scan->mgmtData = (void *) s_mgmt;
        r_mgmt=(RM*)rel->mgmtData;
    if(r_mgmt->latest_slot.slot>s_mgmt->cur_id.slot){
    	return RC_OK;
    }else{
        return RC_RM_NO_MORE_TUPLES;}
    //	printf("t1:::%d\n",te->latest_slot.page);
    //	printf("t2:::%d\n",te->latest_slot.slot);
}

// if the cond is fed
int if_find_result(Value *value){
	if(value->v.boolV==1){
		return 1;//yes
	}else{
		return 0;//no
	}
}

//read every records to compare the condition and return
RC next(RM_ScanHandle *scan, Record *record) {
	sm *sm;
	sm =scan->mgmtData;
        Schema *sc=scan->rel->schema;
	RM *rm = (RM *) scan->rel->mgmtData;
	RID *lastRecord = (RID *) malloc(sizeof(RID));
    if(rm->latest_slot.slot==1){
        if(rm->latest_slot.page>1){
            lastRecord->page=rm->latest_slot.page-1;
            lastRecord->slot=rm->slotperpage;
        }else{
            return RC_RM_NO_MORE_TUPLES;
        }
    }
    if(rm->latest_slot.slot!=1){
        lastRecord->page=rm->latest_slot.page;
        //   printf("latest.Page :%d\n",lastRecord->page);

        lastRecord->slot=rm->latest_slot.slot-1;
		//	printf("latest.Slot :%d\n",lastRecord->slot);
    }


	SM_FileHandle fh;
	SM_PageHandle ph;
	int cpage = sm->cur_id.page;
	//printf("Cur.Page :%d\n",cpage);
	int cslot = sm->cur_id.slot;
	//printf("Cur.Slot :%d\n",cslot);
	int record_s = rm->record_size;
	int slot_s = rm->slot_size;
	//printf("size :%d\n",slot_s);
	int i;
        int limit;
	Value *value;
        limit=rm->latest_slot.slot-1;
	ph = (char *) malloc((sizeof(char) * PAGE_SIZE));
	memset(ph, 0, sizeof(char) * PAGE_SIZE);

	openPageFile(rm->table_name, &fh);

	for(cpage;cpage<=lastRecord->page;cpage++){
		readBlock(cpage,&fh,ph);//find cpage page
		if(cpage==lastRecord->page){
			rm->slotperpage=limit;
		}
		for(i=cslot;i<=rm->slotperpage;i++){
			memcpy(record->data,ph+(i-1)*slot_s+sizeof(RID),record_s);
			int a;
			char *c=(char*)malloc(4);
			memcpy(&a,record->data,sizeof(int));
			memcpy(c,record->data+sizeof(int),4);
			//printf("DATA______scan.k1:%d\n",a);
            //	printf("scan.k2:%s\n",c);
			memcpy(&a,record->data+8,sizeof(int));
            //	printf("END______scan.k3:%d\n",a);
			evalExpr(record,sc,sm->cond,&value);
			if(if_find_result(value)==0){
				//printf("go to next slot...\n");
				//printf("current_op_slot: %d\n",i);
			}else{
				if(i==rm->slotperpage){
					sm->cur_id.page=cpage+1;
					sm->cur_id.slot=1;
				}else{
					sm->cur_id.page=cpage;
					sm->cur_id.slot=i+1;
				}
				return RC_OK;
			}
		}
		sm->cur_id.page=cpage+1;
		sm->cur_id.slot=1;

	}
    closePageFile(&fh);
    return RC_RM_NO_MORE_TUPLES;

}

RC closeScan(RM_ScanHandle *scan) {
        printf("sm has been over!!!");
	return RC_OK;
}

// dealing with schemas
int getRecordSize(Schema *schema) {

	int size, i;
	
        size = 0;
    //size + sizeof(RID);
    //  printf("hanshulide: %d\n",123);
	for (i = 0; i < schema->numAttr; i++) {
        // printf("type:%d\n",schema->dataTypes[i]);
        if(schema->dataTypes[i]==0){
            size=size+sizeof(int);
        }
        if(schema->dataTypes[i]==1){
            size=size+schema->typeLength[i];
        }
        if(schema->dataTypes[i]==2){
            size=size+sizeof(int);
        }
        if(schema->dataTypes[i]==3){
            size=size+sizeof(int);
        }
        //printf("size:%d\n",size);
    }
	return size;

}

Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes,
                     int *typeLength, int keySize, int *keys) {

	Schema *schema;
	schema = (Schema *) malloc(sizeof(Schema));
	schema->numAttr = numAttr;
	schema->attrNames = attrNames;
	schema->dataTypes = dataTypes;
	schema->typeLength = typeLength;
	schema->keyAttrs = keys;
	schema->keySize = keySize;
	return schema;
}

RC freeSchema(Schema *schema) {

	free(schema->attrNames);
	free(schema->dataTypes);
	free(schema->typeLength);
	free(schema->keyAttrs);
	free(schema);
	return RC_OK;
}

// dealing with records and attribute values
RC createRecord(Record **record, Schema *schema) {
	Record *r = (Record *) malloc(sizeof(Record));
	char *data = (char *) malloc(sizeof(char) * getRecordSize(schema));
	r->data = data;
	*record = r;
	return RC_OK;
}
RC freeRecord(Record *record) {
	free(record->data);
	free(record);
	return RC_OK;
}
RC getAttr(Record *record, Schema *schema, int attrNum, Value **value) {
	int i = 0;
	int pos = 0;
	for (i = 0; i < attrNum; i++) {
		if(schema->dataTypes[i]==0) {

            pos = pos + sizeof(int);}
        if(schema->dataTypes[i]==1) {
            pos = pos + sizeof(int);

		}

        if(schema->dataTypes[i]==2){
            pos=pos+sizeof(float);

        }
        if(schema->dataTypes[i]==3){
            pos=pos+sizeof(int);
        }
	}
	Value *Attr_Data=(Value*)malloc(sizeof(Value));
	int a;
	float f;
	char *c;
	if(schema->dataTypes[attrNum]==0) {
        memcpy(&a,record->data+ pos,sizeof(int) );
        MAKE_VALUE(Attr_Data, DT_INT, a);}
      // printf("get_v.I:\n %d",a);
    if(schema->dataTypes[attrNum]==1) {

        c = (char *) malloc(sizeof(char) * schema->typeLength[i]);
        memcpy(c,record->data+pos,schema->typeLength[i]);
        c[schema->typeLength[i]]='\0';
        MAKE_STRING_VALUE(Attr_Data,c);
//printf("get_v.S:\n",c);
    }
    if(schema->dataTypes[attrNum]==2) {
        memcpy(&a,record->data+ pos,sizeof(float) );
        MAKE_VALUE(Attr_Data, DT_FLOAT, f);}

    if(schema->dataTypes[attrNum]==3) {
        memcpy(&a,record->data+ pos,sizeof(float) );
        MAKE_VALUE(Attr_Data, DT_BOOL, a);}

    *value = Attr_Data;
	return RC_OK;
}

RC setAttr(Record *record, Schema *schema, int attrNum, Value *value) {
	int i = 0;
	int pos = 0;
	for (i = 0; i < attrNum; i++) {

        if(schema->dataTypes[i]==0){
            pos=pos+sizeof(int);
        }
        if(schema->dataTypes[i]==1){
            pos=pos+schema->typeLength[i];
        }
        if(schema->dataTypes[i]==2){
            pos=pos+sizeof(float);

        }
        if(schema->dataTypes[i]==3){
            pos=pos+sizeof(int);
        }
	}
    if(schema->dataTypes[attrNum]==0){
        memcpy(record->data + pos, &(value->v.intV),sizeof(int));

           //printf("set_v.int: %d\n",value->v.intV);
    }
    if(schema->dataTypes[attrNum]==1){
        memcpy(record->data+pos, value->v.stringV,schema->typeLength[i]);
          printf("set_v.str: %s\n",value->v.stringV);
    }
    if(schema->dataTypes[attrNum]==2){
        memcpy(record->data + pos, &(value->v.floatV),sizeof(float));

        //   printf("int: %d",value->v.intV);
    }
	return RC_OK;
}
