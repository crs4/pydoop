# BEGIN_COPYRIGHT
# 
# Copyright 2009-2014 CRS4.
# 
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
# 
# END_COPYRIGHT

HADOOP_HOME = $(shell python -c 'import pydoop; print pydoop.hadoop_home()')
HADOOP_CONF_DIR = $(shell python -c 'import pydoop; print pydoop.hadoop_conf()')
HADOOP = $(shell python -c 'import pydoop; print pydoop.hadoop_exec()') --config $(HADOOP_CONF_DIR)

PYTHON_ABI_TAG ?= $(shell uname -r | grep gentoo >/dev/null && eselect python show | tr -s python -)
BOOST_PYTHON ?= boost_python$(PYTHON_ABI_TAG)

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
	cd $< && HADOOP_HOME=$(HADOOP_HOME) BOOST_PYTHON=$(BOOST_PYTHON) python setup.py build --build-base $(BASE_BUILD_DIR) --build-lib $(PYDOOP_BUILD_DIR)
	cd $(PYDOOP_DIR) && tar czf pydoop.tgz *

$(CV_TAR): $(CV_TRUNK)/setup.py $(CV_TRUNK)/cv 
	python $< build --build-base $(BASE_BUILD_DIR) --build-lib $(CV_BUILD_DIR)
	cd $(CV_DIR) && tar czf cv.tgz *

upload: pydoop cv dfsclean
	$(HADOOP) dfs -mkdir $(HDFS_WORK_DIR)
	$(HADOOP) dfs -put $(PYDOOP_TAR) $(HDFS_WORK_DIR)
	$(HADOOP) dfs -put $(CV_TAR) $(HDFS_WORK_DIR)

run: self_contained.py upload
	HDFS_WORK_DIR=$(HDFS_WORK_DIR) python $< $(INPUT_DIR)

clean:
	rm -rf $(BASE_BUILD_DIR)

distclean: clean
	find . -regex '.*\(\.pyc\|\.pyo\|~\|\.so\)' -exec rm -fv {} \;
