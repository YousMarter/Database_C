obj=test_assign1_1.o dberror.o storage_mgr.o
out=storage_mgr

main:$(obj)
	cc -o $(out) $(obj)

storage_mgr.o: dberror.h storage_mgr.h
dberror.o: dberror.h
test_assign1_1.o: storage_mgr.h

.PHONY: clean

clean:
	-rm $(out) $(obj)