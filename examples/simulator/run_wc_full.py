from pydoop.mapreduce.simulator import HadoopSimulatorNetwork
from pydoop.mapreduce.pipes import InputSplit
import logging
import os

from check_results import check_results

program_name = '../wordcount/new_api/wordcount-full.py'
data_in = '../input/alice.txt'
output_dir = './output'

data_in_path = os.path.realpath(data_in)
data_in_uri = 'file://' + data_in_path
data_in_size = os.stat(data_in_path).st_size

os.makedirs(output_dir)
output_dir_uri = 'file://' + os.path.realpath(output_dir)

conf = {
  "mapred.job.name": "wordcount",
  "mapred.work.output.dir": output_dir_uri,
  "mapred.task.partition": "0",
  }

input_split = InputSplit.to_string(data_in_uri, 0, data_in_size)
hsn = HadoopSimulatorNetwork(program=program_name, loglevel=logging.INFO)
hsn.run(None, None, conf, input_split=input_split)
counters = hsn.get_counters()

print
print '=' * 30
print 'Counters'
for phase in ['mapping', 'reducing']:
    print
    print '%s counters:' % phase.capitalize()
    for group in counters[phase]:
        print '\tGroup %s' % group
        for c, v in counters[phase][group].iteritems():
            print '\t\t%s: %s' % (c, v)
print

data_out = os.path.join(output_dir, 'part-%05d' % int(conf["mapred.task.partition"]))
if check_results(data_in, data_out):
    print 'All is well!'


