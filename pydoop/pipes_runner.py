from pydoop.text_protocol import text_down_protocol

class pipes_runner(object):
  def __init__(self, program, output_visitor,
               down_protocol=text_down_protocol):
    self.program = program
    self.output_visitor = output_visitor
    self.tmp_filename = 'foo.out'
    self.down_channel = down_protocol(self.program,
                                      out_file=self.tmp_filename)
    # FIXME the following should be done with some metaclass magic...
    for n in ['start', 'abort',
              'set_job_conf', 'set_input_types',
              'run_map', 'run_reduce',
              'reduce_key', 'reduce_value', 'map_item',
              ]:
      self.__setattr__(n, self.down_channel.__getattribute__(n))
  def close(self):
    self.down_channel.close()
    of = open(self.tmp_filename)
    for l in of:
      l = l.strip()
      parts = l.split('\t')
      cmd = parts[0]
      f = self.output_visitor.__getattribute__(cmd)
      f(*parts[1:])
