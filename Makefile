CC ?= cc
AR ?= ar
CFLAGS ?= -Wall -Wextra -g
LDLIBS = -luuid

.PHONY: all clean check test

all: libtwom.a twomtool twomtest

twom.o: twom.c twom.h xxhash.h
	$(CC) $(CFLAGS) -c -o $@ $<

libtwom.a: twom.o
	$(AR) rcs $@ $<

twomtool: twomtool.c twom.h libtwom.a
	$(CC) $(CFLAGS) -o $@ twomtool.c libtwom.a $(LDLIBS)

twomtest: twomtest.c twom.h libtwom.a
	$(CC) $(CFLAGS) -o $@ twomtest.c libtwom.a $(LDLIBS)

check: twomtest
	./twomtest

test: check

clean:
	rm -f *.o libtwom.a twomtool twomtest
