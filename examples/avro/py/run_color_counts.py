from pydoop.mapreduce.simulator import HadoopSimulatorNetwork
from pydoop.mapreduce.pipes import InputSplit
import logging
import os

program_name = './color_counts.py'
data_in = './users.avro'

path = os.path.realpath(data_in)
length = os.stat(path).st_size
input_split = InputSplit.to_string('file://'+path, 0, length)

out_path = os.path.realpath('.')

conf = {
    "mapreduce.task.partition": "0",
    "mapreduce.task.output.dir": 'file://%s' % out_path,
}

hsn = HadoopSimulatorNetwork(program=program_name)
hsn.run(None, None, conf, input_split=input_split)
