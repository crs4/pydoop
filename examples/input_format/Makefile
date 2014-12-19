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

PYINPUTFORMAT_JAR=pydoop-input-formats.jar
INPUT_FORMAT=it.crs4.pydoop.mapred.TextInputFormat
LOGLEVEL=INFO
PROGNAME=input_format_test
JOBNAME=input_format_test_job
DATA := ../input
INPUT=${PROGNAME}_input
OUTPUT=${PROGNAME}_output

pathsearch = $(firstword $(wildcard $(addsuffix /$(1),$(subst :, ,$(PATH)))))
SUBMIT_CMD=pydoop submit

HDFS=$(if $(call pathsearch,hdfs),$(call pathsearch,hdfs) dfs ,\
       $(if $(call pathsearch,hadoop),$(call pathsearch,hadoop) fs ,\
	       HDFS_IS_MISSING))

CLASSPATH=$(shell hadoop classpath)
JC = javac -classpath $(CLASSPATH)
JAVA = java -classpath $(CLASSPATH)

SRC = $(wildcard it/crs4/pydoop/mapred*/TextInputFormat.java)
CLASSES = $(subst .java,.class,$(SRC))

.SUFFIXES: .java .class
.java.class:
	$(JC) $*.java


.PHONY: setup_io run clean distclean dfsclean check_results submit

run: check_result

check_result: submit
	python check_results.py ${DATA} /user/${USER}/${OUTPUT} 


setup_io:
	-${HDFS} -rm -r /user/${USER}/${INPUT}
	-${HDFS} -rm -r /user/${USER}/${OUTPUT}
	-${HDFS} -mkdir /user
	-${HDFS} -mkdir /user/${USER}
	${HDFS} -put ${DATA} ${INPUT}


${PYINPUTFORMAT_JAR}: $(CLASSES)
	jar -cvf $@ $(CLASSES)

submit: ${PYINPUTFORMAT_JAR} setup_io
		${SUBMIT_CMD} --upload-file-to-cache wordcount_rr.py\
                  --libjars ${PYINPUTFORMAT_JAR}\
								  --input-format ${INPUT_FORMAT}\
	                --module wordcount_rr\
									--do-not-use-java-record-reader\
									-D pydoop.input.issplitable=true\
	                --log-level ${LOGLEVEL} --job-name ${JOBNAME}\
		              ${PROGNAME} ${INPUT} ${OUTPUT}

clean:
	rm -f ${PYINPUTFORMAT_JAR}
	find ./it -name '*.class' -exec rm {} \;
