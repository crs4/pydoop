from pydoop.mapreduce.simulator import HadoopSimulatorNetwork
from pydoop.mapreduce.pipes import InputSplit
import logging
import os

program_name = '../wordcount/new_api/wordcount-full.py'
data_in = '../input/alice.txt'
data_out = 'results.txt'

conf = {
  "mapred.job.name": "wordcount",
  "mapred.work.output.dir": 'file:///var/tmp',
  "mapred.task.partition": "0",
  }

path = os.path.realpath(data_in)
length = os.stat(path).st_size
input_split = InputSplit.to_string('file://'+path, 0, length)
hsn = HadoopSimulatorNetwork(program=program_name, loglevel=logging.INFO)
hsn.run(None, None, conf, input_split=input_split)
