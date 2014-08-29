from pydoop.mapreduce.simulator import HadoopSimulatorNetwork
import logging
import os

program_name = '../wordcount/new_api/wordcount-minimal.py'
data_in = '../input/alice.txt'
data_out = 'results.txt'

conf = {
  "mapred.map.tasks": "2",
  "mapred.reduce.tasks": "1",
  "mapred.job.name": "wordcount",
  }
hsn = HadoopSimulatorNetwork(program=program_name, loglevel=logging.INFO)
hsn.run(open(data_in), open(data_out, 'w'), conf)

