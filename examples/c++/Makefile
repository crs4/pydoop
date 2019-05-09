# yum install openssl-devel

CXXFLAGS := -pthread -g -pipe -Iinclude
LDFLAGS := -pthread
LDLIBS := -lcrypto

all: wordcount

wordcount: wordcount.o StringUtils.o SerialUtils.o HadoopPipes.o
	$(CXX) $(LDFLAGS) -o $@ $^ $(LDLIBS)
