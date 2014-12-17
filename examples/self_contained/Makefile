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

pathsearch = $(firstword $(wildcard $(addsuffix /$(1),$(subst :, ,$(PATH)))))

WHEEL_DIR=../../dist
# FIXME -- this should be fixed to be an get from pypi
# FIXME -- note the --pre flag in the pip command below
PIP=pip install --use-wheel --no-index --pre --find-links=${WHEEL_DIR}
SUBMIT_CMD=pydoop submit

HDFS=$(if $(call pathsearch,hdfs),$(call pathsearch,hdfs) dfs ,\
       $(if $(call pathsearch,hadoop),$(call pathsearch,hadoop) fs ,\
	       HDFS_IS_MISSING))


LOGLEVEL=DEBUG
PROGNAME=self_contained
JOBNAME=self_contained
DATA := ../input
INPUT=${PROGNAME}_input
OUTPUT=${PROGNAME}_output

.PHONY: setup_io run clean distclean dfsclean check_results submit

run: check_result

check_result: submit2
	python check_results.py /user/${USER}/${INPUT} /user/${USER}/${OUTPUT} 


setup_io:
	-${HDFS} -rm -r /user/${USER}/${INPUT}
	-${HDFS} -rm -r /user/${USER}/${OUTPUT}
	-${HDFS} -mkdir /user
	-${HDFS} -mkdir /user/${USER}
	${HDFS} -put ${DATA} ${INPUT}


submit: cv.zip pydoop.tgz setup_io
	${SUBMIT_CMD} --python-zip cv.zip --upload-archive-to-cache pydoop.tgz\
                --module cv.mr.main --entry-point main\
                --log-level ${LOGLEVEL} --job-name ${JOBNAME}\
                --no-override-home --no-override-env\
	              ${PROGNAME} ${INPUT} ${OUTPUT}

submit2: cv.tgz pydoop.tgz setup_io
	${SUBMIT_CMD} --upload-archive-to-cache cv.tgz\
                --upload-archive-to-cache pydoop.tgz\
                --module cv.mr.main --entry-point main\
                --log-level ${LOGLEVEL} --job-name ${JOBNAME}\
                --no-override-home --no-override-env\
	              ${PROGNAME} ${INPUT} ${OUTPUT}

cv.zip:
	zip -rq cv.zip cv

cv.tgz:
	cd cv; tar cfz ../cv.tgz . ; cd -

pydoop.tgz:
	${PIP} -t .  pydoop
	cd pydoop; tar cfz ../pydoop.tgz . ; cd -
	rm -rf pydoop pydoop-*

clean:
	rm -rf pydoop.tgz cv.zip


