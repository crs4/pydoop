# BEGIN_COPYRIGHT
# 
# Copyright 2009-2015 CRS4.
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

PYINPUTFORMAT_JAR=pydoop-input-formats.jar
INPUT_FORMAT_MRV1=it.crs4.pydoop.mapred.TextInputFormat
INPUT_FORMAT_MRV2=it.crs4.pydoop.mapreduce.TextInputFormat
LOGLEVEL=INFO
JOBNAME=input_format_test_job
DATA := ../input
WC_MODULE=wordcount_rr
WC_MODULE_FILE=${WC_MODULE}.py
INPUT=${JOBNAME}_input
OUTPUT=${JOBNAME}_output

pathsearch = $(firstword $(wildcard $(addsuffix /$(1),$(subst :, ,$(PATH)))))
SUBMIT_CMD=pydoop submit

HDFS=$(if $(call pathsearch,hdfs),$(call pathsearch,hdfs) dfs ,\
       $(if $(call pathsearch,hadoop),$(call pathsearch,hadoop) fs ,\
	       HDFS_IS_MISSING))
HDFS_RMR=$(if $(call pathsearch,hdfs),$(call pathsearch,hdfs) dfs -rmr,\
	       $(if $(call pathsearch,hadoop),$(call pathsearch,hadoop) fs -rmr,\
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
--log-level ${LOGLEVEL} --job-name ${JOBNAME}


.SUFFIXES: .java .class
.java.class:
	$(JC) $*.java


.PHONY: run clean distclean dfsclean

run: submit_mrv1 submit_mrv2

${PYINPUTFORMAT_JAR}: $(CLASSES)
	jar -cvf $@ $(CLASSES)

data:
	-${HDFS_MKDIR}  /user
	-${HDFS_MKDIR} /user/${USER}
	-${HDFS_RMR} /user/${USER}/${INPUT}
	-${HDFS_RMR} /user/${USER}/${OUTPUT}_*
	${HDFS_PUT} ${DATA} ${INPUT}


submit_mrv1: data ${PYINPUTFORMAT_JAR}
	${SUBMIT_CMD} ${SUBMIT_ARGS} --input-format ${INPUT_FORMAT_MRV1}\
		             ${WC_MODULE} ${INPUT} ${OUTPUT}_MRV1
	python check_results.py ${DATA} /user/${USER}/${OUTPUT}_MRV1


submit_mrv2: data ${PYINPUTFORMAT_JAR}
	${SUBMIT_CMD} ${SUBMIT_ARGS} --input-format ${INPUT_FORMAT_MRV2} --mrv2\
		             ${WC_MODULE} ${INPUT} ${OUTPUT}_MRV2
	python check_results.py ${DATA} /user/${USER}/${OUTPUT}_MRV2


clean:
	rm -f ${PYINPUTFORMAT_JAR}
	find ./it -name '*.class' -exec rm {} \;
