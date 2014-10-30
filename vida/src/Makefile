# C++ FLAGS
BOOSTDIR=./boost
BOOST_INC=$(BOOSTDIR)
BOOST_LIB=$(BOOSTDIR)/stage/lib/libboost_

CC=g++
CXXFLAGS=-O3 -D_FILE_OFSET_BITS=64 -I$(BOOST_INC) -I.
# -msse3 -march=native
CFLAGS=$(CXXFLAGS)
LDFLAGS=$(BOOST_LIB)iostreams.a $(BOOST_LIB)system.a $(BOOST_LIB)thread.a $(BOOST_LIB)timer.a $(BOOST_LIB)chrono.a

UNAME := $(shell uname)
ifeq ($(UNAME), Linux)
LDFLAGS+=-lrt
endif

SOURCES=query.cpp 
HEADERS=person_graph.hpp msbfs.hpp naive.hpp
EXECUTABLE=query
OBJECTS=$(SOURCES:.cpp=.o)
DEPS=$(OBJECTS:.o=.d)

all: $(SOURCES) $(EXECUTABLE)

clean:
	@rm -f $(EXECUTABLE)
	@rm -f *.{o,d}
	@rm -f *.d.*

.cpp.o:
	$(CC) $(CFLAGS) -c -o $@ $<

$(EXECUTABLE): $(OBJECTS) $(HEADERS)
	$(CC) $(OBJECTS) $(LDFLAGS) -o $(EXECUTABLE)

define create-deps
	@$(CC) -x c++ -MM $(CFLAGS) $< | sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' > $@
endef

$(DEPS): $(SOURCES) $(HEADERS)
	$(create-deps)

sinclude $(DEPS)


