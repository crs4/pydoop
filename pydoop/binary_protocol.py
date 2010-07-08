# BEGIN_COPYRIGHT
# END_COPYRIGHT

import sys, subprocess
import _pipes as pp


#-- message codes for the down protocol
START_MESSAGE=0
SET_JOB_CONF=1
SET_INPUT_TYPES=2
RUN_MAP=3
MAP_ITEM=4
RUN_REDUCE=5
REDUCE_KEY=6
REDUCE_VALUE=7
CLOSE=8
ABORT=9

#-- message codes for the up protocol
OUTPUT=50
PARTITIONED_OUTPUT=51
STATUS=52
PROGRESS=53
DONE=54
REGISTER_COUNTER=55
INCREMENT_COUNTER=56


def _true_false_int(v):
  return 1 if v else 0


def serialize(t):
  tt = type(t)
  if tt == int:
    return pp.serialize_int(t)
  if tt == float:
    return pp.serialize_float(t)
  if tt == str:
    return pp.serialize_string(t)


class binary_down_protocol(object):
  def __init__(self, pipes_program, out_file=None):
    self.out_file = out_file
    self.pipes_program = pipes_program
    self.fd = open(self.out_file, "w")
    self.proc = subprocess.Popen([],
                                 executable=self.pipes_program,
                                 bufsize=0,
                                 stdin=subprocess.PIPE,
                                 stdout=self.fd)
    #--
    open_server_socket()
    start_process_in_background()
    open_socket()
    #--
  def __send(self, args):
    for v in args:
      self.proc.stdin.write(serialize(v))
      sys.stderr.write('ready to send: <%s>\n' % v)
  #--
  def start(self):
    self.__send([START_MESSAGE,  0])
  #--
  def close(self):
    self.__send([CLOSE])
    self.proc.wait()
    self.fd.close()
  #--
  def abort(self):
    self.__send([ABORT])
    self.proc.wait()
    self.fd.close()
  #--
  def set_job_conf(self, job_conf_dict):
    args = [SET_JOB_CONF, '%s' % 2*len(job_conf_dict)]
    for k, v in job_conf_dict.iteritems():
      args.append(k)
      args.append(v)
    self.__send(args)
  #--
  def set_input_types(key_type, value_type):
    self.__send([SET_INPUT_TYPES, key_type, value_type])
  #--
  def run_map(self, input_split, num_reduces, piped_input=True):
    self.__send([RUN_MAP, input_split, num_reduces,
                 _true_false_int(piped_input)])
  #--
  def run_reduce(self, n=1, piped_output=True):
    self.__send([RUN_REDUCE, n, _true_false_int(piped_output)])
  #--
  def reduce_key(self, k):
    self.__send([REDUCE_KEY, k])
  #--
  def reduce_value(self, v):
    self.__send([REDUCE_VALUE, v])
  #--
  def map_item(self, k, v):
    self.__send([MAP_ITEM, k, v])


class binary_up_protocol(object):
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
