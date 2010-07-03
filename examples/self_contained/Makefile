# BEGIN_COPYRIGHT
# END_COPYRIGHT

JAVA_HOME ?= /opt/sun-jdk
HADOOP_HOME ?= /opt/hadoop
HADOOP_CONF_DIR ?= $(HADOOP_HOME)/conf

HADOOP = $(HADOOP_HOME)/bin/hadoop --config $(HADOOP_CONF_DIR)


BASE_BUILD_DIR := $(realpath .)/_build

PYDOOP_TRUNK := $(realpath ../..)
CV_TRUNK := $(realpath .)

PYDOOP_BUILD_DIR := $(BASE_BUILD_DIR)/pydoop
PYDOOP_DIR := $(PYDOOP_BUILD_DIR)/pydoop
PYDOOP_TAR := $(PYDOOP_DIR)/pydoop.tgz

CV_BUILD_DIR := $(BASE_BUILD_DIR)/cv
CV_DIR := $(CV_BUILD_DIR)/cv
CV_TAR := $(CV_DIR)/cv.tgz

HDFS_WORK_DIR := test_self_contained


.PHONY: all pydoop cv upload run clean distclean dfsclean

all: upload
pydoop: $(PYDOOP_TAR)
cv: $(CV_TAR)

$(PYDOOP_TAR): $(PYDOOP_TRUNK)
	cd $< && HADOOP_HOME=$(HADOOP_HOME) JAVA_HOME=$(JAVA_HOME) python setup.py build --build-base $(BASE_BUILD_DIR) --build-lib $(PYDOOP_BUILD_DIR)
	cd $(PYDOOP_DIR) && tar czf pydoop.tgz *

$(CV_TAR): $(CV_TRUNK)/setup.py $(CV_TRUNK)/cv 
	python $< build --build-base $(BASE_BUILD_DIR) --build-lib $(CV_BUILD_DIR)
	cd $(CV_DIR) && tar czf cv.tgz *

upload: pydoop cv dfsclean
	$(HADOOP) dfs -mkdir $(HDFS_WORK_DIR)
	$(HADOOP) dfs -put $(PYDOOP_TAR) $(HDFS_WORK_DIR)
	$(HADOOP) dfs -put $(CV_TAR) $(HDFS_WORK_DIR)
	$(HADOOP) dfs -put bin $(HDFS_WORK_DIR)/bin
	$(HADOOP) dfs -put input $(HDFS_WORK_DIR)/input

run: upload
	$(HADOOP) pipes -D mapred.cache.archives=$(HDFS_WORK_DIR)/pydoop.tgz#pydoop,$(HDFS_WORK_DIR)/cv.tgz#cv -conf conf/cv.xml -program $(HDFS_WORK_DIR)/bin/cv -input $(HDFS_WORK_DIR)/input -output $(HDFS_WORK_DIR)/output

clean:
	rm -rf $(BASE_BUILD_DIR)

distclean: clean
	find . -regex '.*\(\.pyc\|\.pyo\|~\|\.so\)' -exec rm -fv {} \;

dfsclean:
	-$(HADOOP) dfs -rmr $(HDFS_WORK_DIR)
