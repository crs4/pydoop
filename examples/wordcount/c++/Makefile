ARCH := amd64-64
HADOOP_BASE := $(HADOOP_HOME)/c++/Linux-$(ARCH)

CXXFLAGS := -I$(HADOOP_BASE)/include
LDFLAGS := -L$(HADOOP_BASE)/lib
LDLIBS := -lhadooppipes -lhadooputils -lpthread


build: WordCount

run: build
	hadoop fs -put WordCount WordCount
	hadoop fs -put ../../input WordCountInput
	hadoop pipes -conf WordCount.xml -input WordCountInput -output WordCountOutput
	hadoop fs -get WordCountOutput WordCountOutput
	hadoop fs -rmr 'WordCount*'

clean:
	rm -rf WordCount WordCountOutput
