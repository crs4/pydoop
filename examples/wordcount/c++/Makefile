# NOTE: with security-enabled Hadoop versions this example compiles
# fine, but running it fails with a "Server failed to authenticate"
# error. For some reason, rebuilding libhadoop{pipes,utils} from
# source solves the problem.

#ARCH := i386-32
ARCH := amd64-64

HADOOP_BASE := $(HADOOP_HOME)/c++/Linux-$(ARCH)
CXXFLAGS := -I$(HADOOP_BASE)/include
LDFLAGS := -L$(HADOOP_BASE)/lib
LDLIBS := -lhadooppipes -lhadooputils -lpthread
INCLUDING_SSL := $(shell grep -rl openssl $(HADOOP_HOME)/src/c++)
ifdef INCLUDING_SSL
	LDLIBS := $(LDLIBS) -lcrypto -lssl
endif

build: WordCount

run: build
	hadoop fs -put WordCount WordCount
	hadoop fs -put ../../input WordCountInput
	hadoop pipes -conf WordCount.xml -input WordCountInput -output WordCountOutput
	hadoop fs -get WordCountOutput WordCountOutput
	hadoop fs -rm -r 'WordCount*'

clean:
	rm -rf WordCount WordCountOutput
