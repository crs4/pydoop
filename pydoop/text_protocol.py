import subprocess
import sys

def _true_false_str(v):
  return 'true' if v else 'false'

class text_down_protocol(object):
  def __init__(self, pipes_program, out_file=None):
    self.out_file = out_file
    self.pipes_program = pipes_program
    self.fd = open(self.out_file, "w")
    self.proc = subprocess.Popen([],
                                 executable=self.pipes_program,
                                 bufsize=0,
                                 stdin=subprocess.PIPE,
                                 stdout=self.fd)
  def __send(self, args):
    s = '\t'.join(args)
    #sys.stderr.write('ready to send: <%s>\n' % s)
    self.proc.stdin.write('%s\n' % s)
  #--
  def start(self):
    self.__send(['start',  '0'])
  #--
  def close(self):
    self.__send(['close'])
    self.proc.wait()
    self.fd.close()
  #--
  def abort(self):
    self.__send(['abort'])
    self.proc.wait()
    self.fd.close()
  #--
  def set_job_conf(self, job_conf_dict):
    args = ['setJobConf', '%s' % 2*len(job_conf_dict)]
    for k, v in job_conf_dict.iteritems():
      args.append(k)
      args.append(v)
    self.__send(args)
  #--
  def set_input_types(key_type, value_type):
    self.__send(['setInputTypes', key_type, value_type])
  #--
  def run_map(self, input_split, num_reduces, piped_input=True):
    self.__send(['runMap', input_split, '%s' % num_reduces,
                 _true_false_str(piped_input)])
  #--
  def run_reduce(self, n=1, piped_output=True):
    self.__send(['runReduce', '%s' % n, _true_false_str(piped_output)])
  #--
  def reduce_key(self, k):
    self.__send(['reduceKey', k])
  #--
  def reduce_value(self, v):
    self.__send(['reduceValue', v])
  #--
  def map_item(self, k, v):
    self.__send(['mapItem', k, v])

#----------------------------------------------------------------------------

class text_up_protocol(object):
  def __init__(self):
    pass
  def output(self, key, value):
    pass
  def partitioned_output(self, reduce, key, value):
    pass
  def done(self):
    pass
  def progress(self, progress):
    pass
  def status(self, message):
    pass
  def register_counter(self, id, group, name):
    pass
  def increment_counter(self, id, amount):
    pass

#----------------------------------------------------------------------------




