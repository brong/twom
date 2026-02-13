# twom - skiplist key-value store with MVCC
#
# Versioning: MAJOR.MINOR.PATCH (semver)
#   Bump MAJOR for ABI-breaking changes
#   Bump MINOR for new features (backward compatible)
#   Bump PATCH for bug fixes
VERSION_MAJOR = 0
VERSION_MINOR = 1
VERSION_PATCH = 0
VERSION = $(VERSION_MAJOR).$(VERSION_MINOR).$(VERSION_PATCH)

CC ?= cc
AR ?= ar
CFLAGS ?= -Wall -Wextra -g
LDLIBS = -luuid

PREFIX ?= /usr/local
LIBDIR ?= $(PREFIX)/lib
INCLUDEDIR ?= $(PREFIX)/include
BINDIR ?= $(PREFIX)/bin
PKGCONFIGDIR ?= $(LIBDIR)/pkgconfig

# Shared library names
SONAME = libtwom.so.$(VERSION_MAJOR)
SOFILE = libtwom.so.$(VERSION_MAJOR).$(VERSION_MINOR).$(VERSION_PATCH)

.PHONY: all clean check test install uninstall twom.pc

all: libtwom.a libtwom.so twomtool twomtest

# Object files
twom.o: twom.c twom.h xxhash.h
	$(CC) $(CFLAGS) -c -o $@ $<

twom.pic.o: twom.c twom.h xxhash.h
	$(CC) $(CFLAGS) -fPIC -c -o $@ $<

# Static library
libtwom.a: twom.o
	$(AR) rcs $@ $<

# Shared library
libtwom.so: twom.pic.o
	$(CC) -shared -Wl,-soname,$(SONAME) -o $(SOFILE) $< $(LDLIBS)
	ln -sf $(SOFILE) $(SONAME)
	ln -sf $(SONAME) $@

# Tools
twomtool: twomtool.c twom.h libtwom.a
	$(CC) $(CFLAGS) -o $@ twomtool.c libtwom.a $(LDLIBS)

twomtest: twomtest.c twom.h libtwom.a
	$(CC) $(CFLAGS) -o $@ twomtest.c libtwom.a $(LDLIBS)

# Tests
check: twomtest
	./twomtest

test: check

# Install
install: libtwom.a libtwom.so twomtool twom.pc
	install -d $(DESTDIR)$(LIBDIR)
	install -m 644 libtwom.a $(DESTDIR)$(LIBDIR)/
	install -m 755 $(SOFILE) $(DESTDIR)$(LIBDIR)/
	ln -sf $(SOFILE) $(DESTDIR)$(LIBDIR)/$(SONAME)
	ln -sf $(SONAME) $(DESTDIR)$(LIBDIR)/libtwom.so
	install -d $(DESTDIR)$(INCLUDEDIR)
	install -m 644 twom.h $(DESTDIR)$(INCLUDEDIR)/
	install -d $(DESTDIR)$(BINDIR)
	install -m 755 twomtool $(DESTDIR)$(BINDIR)/
	install -d $(DESTDIR)$(PKGCONFIGDIR)
	install -m 644 twom.pc $(DESTDIR)$(PKGCONFIGDIR)/

uninstall:
	rm -f $(DESTDIR)$(LIBDIR)/libtwom.a
	rm -f $(DESTDIR)$(LIBDIR)/$(SOFILE)
	rm -f $(DESTDIR)$(LIBDIR)/$(SONAME)
	rm -f $(DESTDIR)$(LIBDIR)/libtwom.so
	rm -f $(DESTDIR)$(INCLUDEDIR)/twom.h
	rm -f $(DESTDIR)$(BINDIR)/twomtool
	rm -f $(DESTDIR)$(PKGCONFIGDIR)/twom.pc

# pkg-config file (generated)
twom.pc:
	@echo 'prefix=$(PREFIX)' > $@
	@echo 'libdir=$(LIBDIR)' >> $@
	@echo 'includedir=$(INCLUDEDIR)' >> $@
	@echo '' >> $@
	@echo 'Name: twom' >> $@
	@echo 'Description: Skiplist key-value store with MVCC' >> $@
	@echo 'Version: $(VERSION)' >> $@
	@echo 'Libs: -L$${libdir} -ltwom -luuid' >> $@
	@echo 'Cflags: -I$${includedir}' >> $@

clean:
	rm -f *.o libtwom.a libtwom.so* $(SOFILE) twomtool twomtest twom.pc
