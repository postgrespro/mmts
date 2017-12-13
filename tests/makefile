CXX=g++
CC=gcc
CFLAGS=-g -Wall -O0 -pthread 

all: dtmbench dtmacid insert

dtmbench: dtmbench.cpp
	$(CXX) $(CFLAGS) -o dtmbench dtmbench.cpp -lpqxx -lpq

dtmacid: dtmacid.cpp
	$(CXX) $(CFLAGS) -o dtmacid dtmacid.cpp -lpqxx -lpq

insert: insert.c
	$(CC) $(CFLAGS) -o insert insert.c -lpq

clean:
	rm -f dtmbench dtmacid
