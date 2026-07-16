# s3m - Parallel S3 Object Manager
# Top-level Makefile: builds all tools into ./bin

CC      ?= gcc
CFLAGS  ?= -O2 -std=gnu11 -Wall -Wextra -pthread -D_GNU_SOURCE
LDFLAGS ?= -pthread
LDLIBS  := -lcurl -lcrypto

BIN     := bin
SRC     := src
OBJ     := obj

# Tools are added here as they are implemented
TOOLS := $(BIN)/s3m-ls $(BIN)/s3m-du $(BIN)/s3m-rm $(BIN)/s3m-ver $(BIN)/s3m-sync

# Shared engine linked into every tool
CORE := $(OBJ)/s3mcore.o

.PHONY: all clean

all: $(TOOLS)

$(BIN) $(OBJ):
	mkdir -p $@

$(OBJ)/s3mcore.o: $(SRC)/s3mcore.c $(SRC)/s3mcore.h | $(OBJ)
	$(CC) $(CFLAGS) -c -o $@ $<

$(BIN)/%: $(SRC)/%.c $(CORE) $(SRC)/s3mcore.h | $(BIN)
	$(CC) $(CFLAGS) -o $@ $< $(CORE) $(LDFLAGS) $(LDLIBS)

clean:
	rm -rf $(BIN) $(OBJ)
