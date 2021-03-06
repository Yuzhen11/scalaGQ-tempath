CCOMPILE=mpic++
PLATFORM=Linux-amd64-64
CPPFLAGS= -I$(HADOOP_HOME)/src/c++/libhdfs -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux -I ~/fan/apps/pregelol
LIB = -L$(HADOOP_HOME)/c++/$(PLATFORM)/lib
LDFLAGS = -lhdfs -Wno-deprecated -O2

file=latestT.cpp

all: run

run: $(file)
	$(CCOMPILE) $(file) $(CPPFLAGS) $(LIB) $(LDFLAGS)  -o run
	./kill.py
clean:
	-rm run
