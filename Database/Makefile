obj=test_assign3_1.o dberror.o storage_mgr.o buffer_mgr_stat.o buffer_mgr.o record_mgr.o rm_serializer.o expr.o
out=record_mgr

objexpr=test_expr.o expr.o record_mgr.o storage_mgr.o rm_serializer.o dberror.o
outexpr=test_expr

objad=test_additional.o dberror.o storage_mgr.o buffer_mgr_stat.o buffer_mgr.o expr.o record_mgr.o rm_serializer.o
outad= ad_test

all: main expr ad



main:$(obj)
	cc -g -o $(out) $(obj)
expr:$(objexpr)
	cc -g -o $(outexpr) $(objexpr)
ad:$(objad)
	cc -g -o $(outad) $(objad)

test_additional.o: record_mgr.h storage_mgr.h buffer_mgr_stat.h buffer_mgr.h dberror.h test_helper.h tables.h
buffer_mgr_stat.o: buffer_mgr.h
record_mgr.o: record_mgr.h buffer_mgr.h
buffer_mgr.o: dberror.h storage_mgr.h dt.h
dberror.o: dberror.h
rm_serializer.o: record_mgr.h tables.h dberror.h
expr.o: expr.h dberror.h tables.h
test_expr.o: expr.h dberror.h tables.h record_mgr.h test_helper.h
storage_mgr.o: storage_mgr.h dberror.h
test_assign3_1.o: record_mgr.h storage_mgr.h buffer_mgr_stat.h buffer_mgr.h dberror.h test_helper.h tables.h

.PHONY: clean

clean:
	rm *.o $(out) $(outexpr) $(outad)