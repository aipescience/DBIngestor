# Copyright (C) 2009, Steffen Knollmann & Adrian Partl

include ../Makefile.config

.PHONY: all clean

# Define the sources
sources = 	dbingestor_error.cpp\
		DataObjDesc.cpp \
		HeaderReader.cpp \
		Reader.cpp \
		Schema.cpp \
		SchemaDataMapGenerator.cpp \
		DBAbstractor.cpp \
		DBIngestor.cpp \
		DBIngestBuffer.cpp \
		DBSqlite3.cpp \
		DBMySQL.cpp \
		SchemaItem.cpp

# Include additional rules
include ../Makefile.rules

# Catch empty call
all:
	$(MAKE) DBIngestor

# Cleaning up
clean:
	rm -f DBIngestor  $(sources:.cpp=.o) $(sources:.cpp=.d)

# How to build the main programs
DBIngestor: $(sources:.cpp=.o) \
#        ../version.h 
	$(CC) $(CFLAGS) -o DBIngestor $(sources:.cpp=.o) \
	                        $(LIBS)  -lm

# Now include all objects
-include $(sources:.cpp=.d)

# External dependencies
#libadts/libadts.a:
#	$(MAKE) -C libadts
#
#libutil/libutil.a:
#	$(MAKE) -C libutil
#
#libio/libio.a:
#	$(MAKE) -C libio
