#!/bin/bash

MODULE=foobar
MZIP=${MODULE}.zip
MPY=${MODULE}.py

PROGNAME=${MODULE}-prog
JOBNAME=${MODULE}-job

LOGLEVEL=DEBUG

INPUT=input
OUTPUT=output

SUBMIT_CMD="/home/zag/.local/bin/pydoop submit"

cat <<EOF > ${MPY}
import itertools as it
from pydoop.mapreduce.pipes import run_task, Factory
from pydoop.mapreduce.api import Mapper, Reducer

class TMapper(Mapper):
    def map(self, ctx):
        words = ''.join(c for c in ctx.value
                        if c.isalnum() or c == ' ').lower().split()
        for w in words:
            ctx.emit(w, '1')

class TReducer(Reducer):
    def reduce(self, ctx):
        s = sum(it.imap(int, ctx.values))
        ctx.emit(ctx.key, str(s))

def main():
    run_task(Factory(mapper_class=TMapper, reducer_class=TReducer))
EOF

zip ${MZIP} ${MPY}

hdfs dfs -mkdir -p /user/${USER}/${INPUT}
hdfs dfs -rmdir /user/${USER}/${OUTPUT}
hdfs dfs -put -f ${MPY} ${INPUT}

${SUBMIT_CMD} --python-egg ${MZIP} --wrap ${MODULE} \
              --log-level ${LOGLEVEL} --job-name ${JOBNAME} \
              ${PROGNAME} ${INPUT} ${OUTPUT} 
