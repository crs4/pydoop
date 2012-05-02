ARCH := amd64-64
HADOOP_BASE := $(HADOOP_HOME)/c++/Linux-$(ARCH)

CXXFLAGS := -I$(HADOOP_BASE)/include
LDFLAGS := -L$(HADOOP_BASE)/lib
LDLIBS := -lhadooppipes -lhadooputils -lpthread


build: WordCount

clean:
	rm -f WordCount
