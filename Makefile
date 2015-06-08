# Compiler flags
ifeq ($(CC),cc)
	CC=g++
endif

BOOSTDIR=./boost
BOOST_INC=$(BOOSTDIR)
BOOST_LIB=$(BOOSTDIR)/stage/lib/libboost_
BOOST_LIBS=$(BOOST_LIB)iostreams.a $(BOOST_LIB)system.a $(BOOST_LIB)thread.a $(BOOST_LIB)timer.a $(BOOST_LIB)chrono.a
LDFLAGS=$(BOOST_LIBS)
UNAME := $(shell uname)
ifeq ($(UNAME), Linux)
LDFLAGS+=-lrt
endif

#Profiling CFLAGS
MATH_FLAGS=-ffast-math -funsafe-math-optimizations -fassociative-math -ffinite-math-only -fno-signed-zeros
ifeq ($(CC),g++)
	COMPILER_FLAGS=-fabi-version=6
else
	COMPILER_FLAGS=
endif
BASE_FLAGS=-g -std=c++11 -W -Wall -Wextra -pedantic $(MATH_FLAGS) -funroll-all-loops -fno-omit-frame-pointer $(COMPILER_FLAGS)

LOCAL_ARCH=-march=native
RELEASE_ARCH=-march=native
CFLAGS=$(LOCAL_ARCH) -O3 $(BASE_FLAGS) -DNDEBUG
RELEASE_CFLAGS=$(RELEASE_ARCH) -O3 $(BASE_FLAGS) -DNDEBUG -DNDBGPRINT
LD_FLAGS=-Wl,-O1 -pthread

# Source / Executable Variables
CORE_SOURCES=graph.cpp io.cpp log.cpp scheduler.cpp bfs/naive.cpp bfs/sc2012.cpp bfs/parabfs.cpp bfs/noqueue.cpp bfs/batch64.cpp bfs/batch128.cpp bfs/batch256.cpp bfs/sse.cpp worker.cpp query4.cpp 
ALL_SOURCES=main.cpp $(CORE_SOURCES)
CORE_OBJECTS=$(addsuffix .o, $(basename $(CORE_SOURCES)))
CORE_DEPS=$(addsuffix .depends, $(basename $(ALL_SOURCES)))

EXECUTABLE_BENCHER=runBencher 
EXECUTABLE_BENCH=runBench
EXECUTABLE_BENCH_PROFILE=runBenchProfile
EXECUTABLE_FAST=runBfs 
EXECUTABLE_DEBUG=runBfsDebug 
ifndef DEBUG
EXEC_EXECUTABLE=$(EXECUTABLE_FAST)
else
EXEC_EXECUTABLE=$(EXECUTABLE_DEBUG)
endif

RELEASE_OBJECTS=$(addsuffix .release.o, $(basename $(CORE_SOURCES)))

# Testing related variables
TEST_DATA_PATH=test_data
10K_DATASET_PATH=$(TEST_DATA_PATH)/data10k/person_knows_person.csv
1K_DATASET_PATH=$(TEST_DATA_PATH)/data1k/person_knows_person.csv
100K_QUERIES=test_queries/test_100k.txt
10K_QUERIES=test_queries/ldbc10k.txt
1K_QUERIES=test_queries/ldbc1k.txt

# Program rules
.PHONY: test_env test_all

all: $(EXEC_EXECUTABLE) $(EXECUTABLE_BENCHER)
	@rm -f $(CORE_DEPS)

test_all: test_1k test_10k

test_10k: test_env $(EXEC_EXECUTABLE)
	@rm -f $(CORE_DEPS)
	$(TEST_PREF) ./$(EXEC_EXECUTABLE) $(10K_QUERIES) 3 1

test_1k: test_env $(EXEC_EXECUTABLE)
	@rm -f $(CORE_DEPS)
	$(TEST_PREF) ./$(EXEC_EXECUTABLE) $(1K_QUERIES)

clean:
	-rm $(EXECUTABLE_FAST) $(EXECUTABLE_DEBUG) $(EXECUTABLE_BENCH_PROFILE) $(EXECUTABLE_BENCH) $(EXECUTABLE_BENCHER)
	-rm $(CORE_OBJECTS) $(RELEASE_OBJECTS) *.o
	-rm $(CORE_DEPS)
	-rm *.gcda util/*.gdca

$(EXECUTABLE_BENCH): $(ALL_SOURCES)
	$(CC) $(RELEASE_CFLAGS) $(CORE_SOURCES) -fprofile-generate $< -o $(EXECUTABLE_BENCH_PROFILE) $(LD_FLAGS) $(LIBS)
	./$(EXECUTABLE_BENCH_PROFILE) $(10K_QUERIES) 1 1
	$(CC) $(RELEASE_CFLAGS) $(CORE_SOURCES) -fprofile-use $< -o $(EXECUTABLE_BENCH) $(LD_FLAGS) $(LIBS)
	-rm *.gcda util/*.gcda
	-rm $(EXECUTABLE_BENCH_PROFILE)

$(EXECUTABLE_BENCH_THREADS): benchThreads.release.o $(RELEASE_OBJECTS) $(BOOST_LIBS)
	@rm -f $(CORE_DEPS)
	$(CC) benchThreads.release.o $(RELEASE_OBJECTS) $(LDFLAGS) -o $@ $(LD_FLAGS) $(LIBS)

$(EXECUTABLE_BENCHER): runBencher.release.o $(RELEASE_OBJECTS) $(BOOST_LIBS)
	@rm -f $(CORE_DEPS)
	$(CC) runBencher.release.o $(RELEASE_OBJECTS) $(LDFLAGS) -o $@ $(LD_FLAGS) $(LIBS)

$(EXECUTABLE_BENCH_VARIANTS): benchVariants.release.o $(RELEASE_OBJECTS) $(BOOST_LIBS)
	@rm -f $(CORE_DEPS)
	$(CC) benchVariants.release.o $(RELEASE_OBJECTS) $(LDFLAGS) -o $@ $(LD_FLAGS) $(LIBS)

ifndef DEBUG
$(EXEC_EXECUTABLE): main.release.o $(RELEASE_OBJECTS) $(BOOST_LIBS)
	@rm -f $(CORE_DEPS)
	$(CC) main.release.o $(RELEASE_OBJECTS) $(LDFLAGS) -o $@ $(LD_FLAGS) $(LIBS)
else
$(EXEC_EXECUTABLE): main.o $(CORE_OBJECTS)
	@rm -f $(CORE_DEPS)
	$(CC) main.o $(CORE_OBJECTS) -o $@ $(LD_FLAGS) $(LIBS)
endif

%.release.o: %.cpp
	$(CC) $(RELEASE_CFLAGS) $(LDFLAGS) -c $< -o $@ $(LIBS)

.cpp.o:
	$(CC) $(CFLAGS) -DTRACE -c $< -o $@ $(LIBS)


%.depends: %.cpp
	@$(CC) -M $(CFLAGS) -c $< > $@ $(LIBS)

# Test data rules
test_env: $(TEST_DATA_PATH)

-include $(CORE_DEPS)
