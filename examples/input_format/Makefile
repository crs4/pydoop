# BEGIN_COPYRIGHT
# 
# Copyright 2009-2018 CRS4.
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

PYTHON ?= python
PY_VER = $(shell $(PYTHON) -c 'import sys; print(sys.version_info[0])')
PYINPUTFORMAT_JAR=pydoop-input-formats.jar
INPUT_FORMAT=it.crs4.pydoop.mapreduce.TextInputFormat
LOGLEVEL=INFO
JOBNAME=input_format_test_job
DATA := ../input
WC_MODULE=wordcount_minimal
WC_MODULE_FILE=../wordcount/bin/${WC_MODULE}.py
INPUT=${JOBNAME}_input
OUTPUT=${JOBNAME}_output

pathsearch = $(firstword $(wildcard $(addsuffix /$(1),$(subst :, ,$(PATH)))))
SUBMIT_CMD = pydoop$(PY_VER) submit

HDFS=$(if $(call pathsearch,hdfs),$(call pathsearch,hdfs) dfs ,\
       $(if $(call pathsearch,hadoop),$(call pathsearch,hadoop) fs ,\
	       HDFS_IS_MISSING))
HDFS_RMR=$(if $(call pathsearch,hdfs),$(call pathsearch,hdfs) dfs -rm -r,\
	       $(if $(call pathsearch,hadoop),$(call pathsearch,hadoop) fs -rm -r,\
	       HDFS_IS_MISSING))
HDFS_PUT=${HDFS} -put
HDFS_MKDIR=${HDFS} -mkdir

CLASSPATH=$(shell hadoop classpath)
JC = javac -classpath $(CLASSPATH)
JAVA = java -classpath $(CLASSPATH)

SRC = $(wildcard it/crs4/pydoop/mapred*/TextInputFormat.java)
CLASSES = $(subst .java,.class,$(SRC))

SUBMIT_ARGS = --upload-file-to-cache ${WC_MODULE_FILE} \
--libjars ${PYINPUTFORMAT_JAR}\
-D pydoop.input.issplitable=true\
--log-level ${LOGLEVEL} --job-name ${JOBNAME} --entry-point main


.SUFFIXES: .java .class
.java.class:
	$(JC) $*.java


.PHONY: run clean distclean dfsclean

run: submit

${PYINPUTFORMAT_JAR}: $(CLASSES)
	jar -cvf $@ $(CLASSES)

data:
	-${HDFS_MKDIR}  /user
	-${HDFS_MKDIR} /user/${USER}
	-${HDFS_RMR} /user/${USER}/${INPUT}
	-${HDFS_RMR} /user/${USER}/${OUTPUT}
	${HDFS_PUT} ${DATA} ${INPUT}


submit: data ${PYINPUTFORMAT_JAR}
	${SUBMIT_CMD} ${SUBMIT_ARGS} --input-format ${INPUT_FORMAT} \
		             ${WC_MODULE} ${INPUT} ${OUTPUT}
	$(PYTHON) check_results.py ${DATA} /user/${USER}/${OUTPUT}


clean:
	rm -f ${PYINPUTFORMAT_JAR}
	find ./it -name '*.class' -exec rm {} \;
