# Disque module makefile
# Copyright (C) 2018 Salvatore Sanfilippo -- All Rights Reserved

# find the OS
uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')

# Compile flags for linux / osx
ifeq ($(uname_S),Linux)
	SHOBJ_CFLAGS ?= -W -Wall -fno-common -g -ggdb -std=c11 -O2
	SHOBJ_LDFLAGS ?= -shared
else
	SHOBJ_CFLAGS ?= -W -Wall -dynamic -fno-common -g -ggdb -std=c11 -O2
	SHOBJ_LDFLAGS ?= -bundle -undefined dynamic_lookup
endif

.SUFFIXES: .c .so .xo .o

all: disque.so

.c.xo:
	$(CC) -I. $(CFLAGS) $(SHOBJ_CFLAGS) -fPIC -c $< -o $@

disque.xo: ../redismodule.h

MODULE_OBJECTS=ack.xo job.xo queue.xo sds.xo skiplist.xo cluster.xo utils.xo module.xo aof.xo rax.xo adlist.xo disquecmd.xo

disque.so: $(MODULE_OBJECTS)
	$(LD) -o $@ $(MODULE_OBJECTS) $(SHOBJ_LDFLAGS) $(LIBS) -lc

clean:
	rm -rf *.xo *.so
