CS525 Assignment3

Group members:
Chuang Xie	A20316681
You Wu	A20308578
Dedong Zhao	A23011478
Hao Mei	A20342455

List of all files:
1) Files got from blackboard
2) Files we write:record_mgr.c,test_additional,Makefile, Readme

What record_mgr.c has:

record_mgmt:RM
First we create a record_mgr handle structure to write/read the information of the very first page which is the Table information.
The most important thing in the page is the Schema model and the RID pointer to help the RM to insert/read/delete/update the records.

slot:
we create the slot(tuple) as this model RID+<Record_data>+RID.The first RID is the ID of the record/slot the last RID is the empty place 
where we can find the next empty place to insert the record without create a new slot place in the very last page.This RID more like a link_list
which contains the empty place in the Table.

Records and data:
Records data in our program is saved as the *char.
we have comments almost every place to print the data at the point we want,you can check the data with deleting the comments.

Scan:
we want to read all the records with order to find out if the record feeds the condition.
If yes,return it.
If no,go to next slot.

Move to next Page:
Every Page has limit place to store the slots.Every time we need to go next page the slot will be set 1 as the first slot of the page.
We write the Slot from the page:1,slot:1.



The platform and IDE:
1) Mac OS
2) Xcode
3) Eclipse

The commands to run:
1) make clean
2) make
3) ./record_mgr
4) ./test_expr
5) ./ad_test




