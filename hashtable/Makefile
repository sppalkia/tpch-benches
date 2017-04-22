UNAME_S := $(shell uname -s)

CC=gcc
CXX=g++

ifeq ($(UNAME_S),Darwin)
    COPENMP=clang-omp++ -liomp5 -fopenmp
endif
ifeq ($(UNAME_S),Linux)
    COPENMP=g++ -fopenmp -std=c++11
endif

all: setup

setup.o: setup.cpp
	${COPENMP} -O2 -c setup.cpp -o setup.o

setup: setup.o
	${COPENMP} -O2 setup.o -o setup

