# s3m - Parallel S3 Object Manager
# Top-level Makefile: builds all tools into ./bin

CC      ?= gcc
CFLAGS  ?= -O2 -std=gnu11 -Wall -Wextra -pthread -D_GNU_SOURCE
LDFLAGS ?= -pthread
LDLIBS  := -lcurl -lcrypto

# Suite version: single source of truth is the VERSION file
VERSION  := $(shell cat VERSION)
VERFLAGS := -DS3M_VERSION='"$(VERSION)"'

PREFIX  ?= /usr/local
BINDIR  ?= $(PREFIX)/bin
MANDIR  ?= $(PREFIX)/share/man
DOCDIR  ?= $(PREFIX)/share/doc/s3m

BIN     := bin
SRC     := src
OBJ     := obj

# Tools are added here as they are implemented
TOOLS := $(BIN)/s3m-ls $(BIN)/s3m-du $(BIN)/s3m-rm $(BIN)/s3m-ver $(BIN)/s3m-sync $(BIN)/s3m-cp $(BIN)/s3m-find $(BIN)/s3m-diff

# Shared engine linked into every tool
CORE := $(OBJ)/s3mcore.o

.PHONY: all clean install dist rpm

all: $(TOOLS)

$(BIN) $(OBJ):
	mkdir -p $@

$(OBJ)/s3mcore.o: $(SRC)/s3mcore.c $(SRC)/s3mcore.h | $(OBJ)
	$(CC) $(CFLAGS) -c -o $@ $<

$(BIN)/%: $(SRC)/%.c $(CORE) $(SRC)/s3mcore.h VERSION | $(BIN)
	$(CC) $(CFLAGS) $(VERFLAGS) -o $@ $< $(CORE) $(LDFLAGS) $(LDLIBS)

clean:
	rm -rf $(BIN) $(OBJ) rpmbuild s3m-*.tar.gz

# make install [DESTDIR=...] [PREFIX=/usr]
install: all
	install -d $(DESTDIR)$(BINDIR) $(DESTDIR)$(MANDIR)/man1 \
	    $(DESTDIR)$(MANDIR)/man7 $(DESTDIR)$(DOCDIR)
	install -m 0755 $(TOOLS) $(DESTDIR)$(BINDIR)
	for m in man/*.1; do \
	    sed 's/@VERSION@/$(VERSION)/' $$m \
	        > $(DESTDIR)$(MANDIR)/man1/$$(basename $$m); \
	    chmod 0644 $(DESTDIR)$(MANDIR)/man1/$$(basename $$m); \
	done
	for m in man/*.7; do \
	    sed 's/@VERSION@/$(VERSION)/' $$m \
	        > $(DESTDIR)$(MANDIR)/man7/$$(basename $$m); \
	    chmod 0644 $(DESTDIR)$(MANDIR)/man7/$$(basename $$m); \
	done
	install -m 0644 README.md CHANGELOG.md LICENSE.md $(DESTDIR)$(DOCDIR)
	install -d $(DESTDIR)$(DOCDIR)/docs
	install -m 0644 docs/*.md $(DESTDIR)$(DOCDIR)/docs

# Source tarball of the committed tree: s3m-VERSION.tar.gz
dist:
	git archive --format=tar --prefix=s3m-$(VERSION)/ HEAD \
	    | gzip -9 > s3m-$(VERSION).tar.gz

# Binary and source RPMs under ./rpmbuild (needs rpm-build)
rpm: dist
	mkdir -p rpmbuild/SOURCES rpmbuild/SPECS
	cp s3m-$(VERSION).tar.gz rpmbuild/SOURCES/
	sed 's/@VERSION@/$(VERSION)/' packaging/s3m.spec.in \
	    > rpmbuild/SPECS/s3m.spec
	rpmbuild -ba --define "_topdir $(CURDIR)/rpmbuild" \
	    rpmbuild/SPECS/s3m.spec
