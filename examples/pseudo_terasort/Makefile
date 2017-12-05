# BEGIN_COPYRIGHT
# 
# Copyright 2009-2016 CRS4.
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
LOGLEVEL=INFO
GENRECORDS_INPUT=genrecords_input
GENRECORDS_OUTPUT=genrecords_output
SORTRECORDS_OUTPUT=sortrecords_output
CHECKRECORDS_OUTPUT=checkrecords_output

pathsearch = $(firstword $(wildcard $(addsuffix /$(1),$(subst :, ,$(PATH)))))
SUBMIT_CMD = pydoop$(PY_VER) submit

HDFS=$(if $(call pathsearch,hdfs),$(call pathsearch,hdfs) dfs ,\
       $(if $(call pathsearch,hadoop),$(call pathsearch,hadoop) fs ,\
	       HDFS_IS_MISSING))
HDFS_RMR=$(if $(call pathsearch,hdfs),$(call pathsearch,hdfs) dfs -rm -r,\
	       $(if $(call pathsearch,hadoop),$(call pathsearch,hadoop) fs -rmr,\
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
	${PYTHON} genrecords.py  --log-level ${LOGLEVEL}\
       --libjars pydoop-input-formats.jar\
       --num-records 1000000\
       /user/${USER}/${GENRECORDS_INPUT} /user/${USER}/${GENRECORDS_OUTPUT}

sortrecords:
	${PYTHON} sortrecords.py --log-level ${LOGLEVEL}\
                           --sampled-records 10000\
                           --num-reducers 8\
		                       /user/${USER}/${GENRECORDS_OUTPUT}\
                           /user/${USER}/${SORTRECORDS_OUTPUT}


checkrecords:
	${PYTHON} checkrecords.py --log-level ${LOGLEVEL}\
                           /user/${USER}/${SORTRECORDS_OUTPUT}\
		                       /user/${USER}/${CHECKRECORDS_OUTPUT}



clean:
	rm -f ${PYINPUTFORMAT_JAR}
	find ./it -name '*.class' -exec rm {} \;
