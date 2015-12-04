obj=test_assign2_1.o storage_mgr.o dberror.o buffer_mgr_stat.o buffer_mgr.o
out=buffer_mgr

main:$(obj)
	cc -g -o $(out) $(obj)

buffer_mgr.o: storage_mgr.h buffer_mgr.h
buffer_mgr_stat.o: buffer_mgr_stat.h buffer_mgr.h
dberror.o: dberror.h
storage_mgr.o: storage_mgr.h dberror.h
test_assign2_1.o: storage_mgr.h buffer_mgr_stat.h buffer_mgr.h dberror.h test_helper.h

.PHONY: clean

clean:
	-rm $(out) $(obj)