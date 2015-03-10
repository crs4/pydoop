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

pathsearch = $(firstword $(wildcard $(addsuffix /$(1),$(subst :, ,$(PATH)))))

WHEEL_DIR=../../dist
# FIXME -- this should be fixed to be an get from pypi
# FIXME -- note the --pre flag in the pip command below
PIP=pip install --use-wheel --no-index --pre --find-links=${WHEEL_DIR}
SUBMIT_CMD=pydoop submit

HDFS=$(if $(call pathsearch,hdfs),$(call pathsearch,hdfs) dfs ,\
       $(if $(call pathsearch,hadoop),$(call pathsearch,hadoop) fs ,\
	       HDFS_IS_MISSING))


LOGLEVEL=INFO
PROGNAME=self_contained
JOBNAME=self_contained
DATA := ../input
INPUT=${PROGNAME}_input
OUTPUT=${PROGNAME}_output

.PHONY: setup_io run clean distclean dfsclean check_results submit

run: check_result

check_result: submit
	python check_results.py /user/${USER}/${INPUT} /user/${USER}/${OUTPUT} 

setup_io:
	-${HDFS} -rmr /user/${USER}/${INPUT}
	-${HDFS} -rmr /user/${USER}/${OUTPUT}
	-${HDFS} -mkdir /user
	-${HDFS} -mkdir /user/${USER}
	${HDFS} -put ${DATA} ${INPUT}


submit: vowelcount.zip pydoop.tgz setup_io
	${SUBMIT_CMD} --python-zip vowelcount.zip \
                --upload-archive-to-cache pydoop.tgz\
                --log-level ${LOGLEVEL} --job-name ${JOBNAME}\
                --no-override-home --no-override-env\
                vowelcount.mr.main --entry-point main \
	              ${INPUT} ${OUTPUT}

submit2: vowelcount.tgz pydoop.tgz setup_io
	${SUBMIT_CMD} --upload-archive-to-cache vowelcount.tgz\
                --upload-archive-to-cache pydoop.tgz\
                --module vowelcount.mr.main --entry-point main\
                --log-level ${LOGLEVEL} --job-name ${JOBNAME}\
                --no-override-home --no-override-env\
	              ${PROGNAME} ${INPUT} ${OUTPUT}

vowelcount.zip: vowelcount
	zip -rq vowelcount.zip vowelcount

vowelcount.tgz:
	cd vowelcount; tar cfz ../vowelcount.tgz . ; cd -

# don't use $(MAKE) -C ../.. wheel, top Makefile is only present in git repo
pydoop.tgz:
	cd ../..; python setup.py bdist_wheel --dist-dir=./dist; cd -
	${PIP} -t .  pydoop
	cd pydoop; tar cfz ../pydoop.tgz . ; cd -
	rm -rf pydoop pydoop-*

clean:
	rm -rf pydoop.tgz vowelcount.zip
