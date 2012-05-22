# BEGIN_COPYRIGHT
# END_COPYRIGHT

HADOOP_HOME = $(shell python -c 'import pydoop; print pydoop.hadoop_home()')
HADOOP_CONF_DIR = $(shell python -c 'import pydoop; print pydoop.hadoop_conf()')
HADOOP = $(shell python -c 'import pydoop.hadoop_utils as hu; print hu.get_hadoop_exec()') --config $(HADOOP_CONF_DIR)

BASE_BUILD_DIR := $(realpath .)/_build

PYDOOP_TRUNK := $(realpath ../..)
CV_TRUNK := $(realpath .)

PYDOOP_BUILD_DIR := $(BASE_BUILD_DIR)/pydoop
PYDOOP_DIR := $(PYDOOP_BUILD_DIR)/pydoop
PYDOOP_TAR := $(PYDOOP_DIR)/pydoop.tgz

CV_BUILD_DIR := $(BASE_BUILD_DIR)/cv
CV_DIR := $(CV_BUILD_DIR)/cv
CV_TAR := $(CV_DIR)/cv.tgz

HDFS_WORK_DIR := pydoop_test_self_contained
INPUT_DIR := ../input


.PHONY: all pydoop cv upload run get clean distclean dfsclean

all: upload
pydoop: $(PYDOOP_TAR)
cv: $(CV_TAR)

$(PYDOOP_TAR): $(PYDOOP_TRUNK)
	cd $< && HADOOP_HOME=$(HADOOP_HOME) python setup.py build --build-base $(BASE_BUILD_DIR) --build-lib $(PYDOOP_BUILD_DIR)
	cd $(PYDOOP_DIR) && tar czf pydoop.tgz *

$(CV_TAR): $(CV_TRUNK)/setup.py $(CV_TRUNK)/cv 
	python $< build --build-base $(BASE_BUILD_DIR) --build-lib $(CV_BUILD_DIR)
	cd $(CV_DIR) && tar czf cv.tgz *

upload: pydoop cv dfsclean
	$(HADOOP) dfs -mkdir $(HDFS_WORK_DIR)
	$(HADOOP) dfs -put $(PYDOOP_TAR) $(HDFS_WORK_DIR)
	$(HADOOP) dfs -put $(CV_TAR) $(HDFS_WORK_DIR)
	$(HADOOP) dfs -put bin $(HDFS_WORK_DIR)/bin
	$(HADOOP) dfs -put $(INPUT_DIR) $(HDFS_WORK_DIR)/input

run: self_contained.py upload
	HDFS_WORK_DIR=$(HDFS_WORK_DIR) python $< $(INPUT_DIR) $(HDFS_WORK_DIR)/output

clean:
	rm -rf $(BASE_BUILD_DIR) output

distclean: clean
	find . -regex '.*\(\.pyc\|\.pyo\|~\|\.so\)' -exec rm -fv {} \;

dfsclean:
	-$(HADOOP) dfs -rmr $(HDFS_WORK_DIR)
