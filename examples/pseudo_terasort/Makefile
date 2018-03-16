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

USER ?= $(shell whoami)
PYTHON ?= python
NUM_RECORDS ?= 1000000
SAMPLED_RECORDS ?= 10000
NUM_MAPS ?= 2
NUM_REDUCERS ?= 2
PYINPUTFORMAT_JAR=pydoop-input-formats.jar
LOGLEVEL ?= INFO
GENRECORDS_INPUT ?= genrecords_input
GENRECORDS_OUTPUT ?= genrecords_output
SORTRECORDS_OUTPUT ?= sortrecords_output
CHECKRECORDS_OUTPUT ?= checkrecords_output

pathsearch = $(firstword $(wildcard $(addsuffix /$(1),$(subst :, ,$(PATH)))))

HDFS=$(if $(call pathsearch,hdfs),$(call pathsearch,hdfs) dfs ,\
       $(if $(call pathsearch,hadoop),$(call pathsearch,hadoop) fs ,\
	       HDFS_IS_MISSING))
HDFS_RMR=$(if $(call pathsearch,hdfs),$(call pathsearch,hdfs) dfs -rm -r,\
	       $(if $(call pathsearch,hadoop),$(call pathsearch,hadoop) fs -rm -r,\
	       HDFS_IS_MISSING))
HDFS_PUT=${HDFS} -put
HDFS_MKDIR=${HDFS} -mkdir

PTERASORT_PATH=it/crs4/pydoop/examples/pterasort

CLASSPATH=$(shell hadoop classpath)
JC = javac -classpath $(CLASSPATH)
JAVA = java -classpath $(CLASSPATH)

SRC = $(wildcard ${PTERASORT_PATH}/*.java)
CLASSES = $(subst .java,.class,$(SRC))


.PHONY: clean distclean dfsclean


${PYINPUTFORMAT_JAR}:
	${JC} ${SRC}
	jar -cvf $@ $(CLASSES)

data:
	-${HDFS_MKDIR}  /user || :
	-${HDFS_MKDIR} /user/${USER} || :
	-${HDFS_MKDIR} /user/${USER}/${GENRECORDS_INPUT} || :
	-${HDFS_RMR} /user/${USER}/${GENRECORDS_OUTPUT} || :
	-${HDFS_RMR} /user/${USER}/${SORTRECORDS_OUTPUT} || :
	-${HDFS_RMR} /user/${USER}/${CHECKRECORDS_OUTPUT} || :

genrecords: data ${PYINPUTFORMAT_JAR}
	${PYTHON} genrecords.py --log-level ${LOGLEVEL}\
       --libjars ${PYINPUTFORMAT_JAR}\
       --num-records ${NUM_RECORDS} --num-maps ${NUM_MAPS}\
       /user/${USER}/${GENRECORDS_INPUT} /user/${USER}/${GENRECORDS_OUTPUT}

sortrecords:
	${PYTHON} sortrecords.py --log-level ${LOGLEVEL}\
                           --sampled-records ${SAMPLED_RECORDS}\
                           --num-reducers ${NUM_REDUCERS}\
		                       /user/${USER}/${GENRECORDS_OUTPUT}\
                           /user/${USER}/${SORTRECORDS_OUTPUT}

checkrecords:
	${PYTHON} checkrecords.py --log-level ${LOGLEVEL}\
                           /user/${USER}/${SORTRECORDS_OUTPUT}\
                           --num-reducers ${NUM_REDUCERS}\
		                       /user/${USER}/${CHECKRECORDS_OUTPUT}

clean:
	rm -f ${PYINPUTFORMAT_JAR}
	find ./it -name '*.class' -exec rm {} \;
