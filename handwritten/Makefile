UNAME_S := $(shell uname -s)

CC=gcc
CXX=g++-6 -std=c++11 -fopenmp

ifeq ($(UNAME_S),Darwin)
    COPENMP=clang-omp++ -liomp5 -fopenmp
endif
ifeq ($(UNAME_S),Linux)
    COPENMP=g++ -fopenmp -std=c++11 -march=native
endif

EXEC_Q6=q6
EXEC_Q1=q1
EXEC_STREAM=stream

FLAGS=-Werror -mavx -march=native -O3 -std=c99 -D_BSD_SOURCE -D_GNU_SOURCE

.PHONY: all q1 q6 stream clean

all: q14

utilold.o: utilold.cpp
	${CXX} -O3 -c utilold.cpp -o utilold.o

utils.o: utils.cpp
	${CXX} -O3 -c utils.cpp -o utils.o

q3-serial.o: q3.cpp
	${CXX} -O3 -c q3.cpp -o q3-serial.o

q3-serial: q3-serial.o utils.o
	${CXX} q3-serial.o utils.o -o q3-serial

q3.o: q3.cpp
	${COPENMP} -O3 -c q3.cpp -o q3.o

q3: q3.o utils.o
	${COPENMP} -O3 q3.o utils.o -o q3

q1.o: q1.cpp
	${COPENMP} -O3 -march=native -c q1.cpp -o q1.o

q1: q1.o utils.o
	${COPENMP} -O3 -flto q1.o utils.o -o q1

q6.o: q6.cpp
	${COPENMP} -O3 -mavx -march=native -c q6.cpp -o q6.o

q6: q6.o utilold.o
	${COPENMP} -O3 -flto q6.o utilold.o -o q6

q12-serial.o: q12.cpp
	${CXX} -O3 -c q12.cpp -o q12-serial.o

q12-serial: q12-serial.o utils.o
	${CXX} q12-serial.o utils.o -o q12-serial

q12.o: q12.cpp
	${COPENMP} -O3 -c q12.cpp -o q12.o

q12: q12.o utils.o
	${COPENMP} -O3 q12.o utils.o -o q12

q14.o: q14.cpp
	${CXX} -O3 -c q14.cpp -o q14.o

q14: q14.o utils.o
	${CXX} q14.o utils.o -o q14

q19.o: q19.cpp
	${COPENMP} -O3 -c q19.cpp -o q19.o

q19: q19.o utils.o
	${COPENMP} q19.o utils.o -o q19

stream:
	${CC} ${FLAGS} stream.c -o ${EXEC_STREAM} `pkg-config --libs libbsd`

clean:
	rm -f ${EXEC_Q1} ${EXEC_Q6} ${EXEC_STREAM} q3 q12 q14 q19 *.o 

