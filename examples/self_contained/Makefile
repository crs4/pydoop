JAVA_HOME ?= /opt/sun-jdk
HADOOP_HOME ?= /opt/hadoop
HADOOP_CONF_DIR ?= $(HADOOP_HOME)/conf

HADOOP = $(HADOOP_HOME)/bin/hadoop --config $(HADOOP_CONF_DIR)


PYDOOP_TRUNK := $(realpath ../..)
BUILD_DIR := $(realpath .)/_build
PYDOOP_DIR := $(BUILD_DIR)/pydoop
CV_DIR := $(BUILD_DIR)/cv

HDFS_WORK_DIR := test_self_contained


.PHONY: all pydoop cv upload run clean distclean dfsclean


all: upload

pydoop:
	cd $(PYDOOP_TRUNK) && python setup.py build --build-base $(BUILD_DIR) --build-lib $(PYDOOP_DIR)
	cd $(PYDOOP_DIR)/pydoop && tar czf pydoop.tgz *

cv: 
	python setup.py build --build-base $(BUILD_DIR) --build-lib $(CV_DIR)
	cd $(CV_DIR)/cv && tar czf cv.tgz *

upload: pydoop cv
	$(HADOOP) dfs -mkdir $(HDFS_WORK_DIR)
	$(HADOOP) dfs -put $(PYDOOP_DIR)/pydoop/pydoop.tgz $(HDFS_WORK_DIR)
	$(HADOOP) dfs -put $(CV_DIR)/cv/cv.tgz $(HDFS_WORK_DIR)
	$(HADOOP) dfs -put bin $(HDFS_WORK_DIR)/bin
	$(HADOOP) dfs -put input $(HDFS_WORK_DIR)/input

run: upload
	$(HADOOP) pipes -D mapred.cache.archives=$(HDFS_WORK_DIR)/pydoop.tgz#pydoop,$(HDFS_WORK_DIR)/cv.tgz#cv -conf conf/cv.xml -program $(HDFS_WORK_DIR)/bin/cv -input $(HDFS_WORK_DIR)/input -output $(HDFS_WORK_DIR)/output

clean:
	rm -rf $(BUILD_DIR)

distclean: clean
	find . -regex '.*\(\.pyc\|\.pyo\|~\|\.so\)' -exec rm -fv {} \;

dfsclean:
	-$(HADOOP) dfs -rmr $(HDFS_WORK_DIR)
