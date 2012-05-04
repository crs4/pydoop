# BEGIN_COPYRIGHT
# END_COPYRIGHT

import sys
from pydoop.pipes_runner import pipes_runner


class output_visitor(object):
  def __init__(self):
    pass
  def output(self, key, value):
    print 'output pair -> <%s,%s>' % (key, value)
  def done(self):
    print 'all done'
  def progress(self, progress):
    print 'we are at %f done.' % float(progress)


def run_map(pipes_exe):
  p = pipes_runner(pipes_exe, output_visitor())
  p.start()
  p.run_map('fake_input_split', 3)
  for v in ['faa', 'fii', 'foo', 'fuu']:
    p.map_item(str(1), str(v))
  p.close()


def run_reduce(pipes_exe):
  p = pipes_runner(pipes_exe, output_visitor())
  p.start()
  p.run_reduce()
  red_vals = {'foo' : range(10), 'bar' : range(14)}
  for k in red_vals.keys():
    p.reduce_key(str(k))
    for v in red_vals[k]:
      p.reduce_value(str(v))
  p.close()


def word_count_example(argv):
  try:
    pipes_exe = sys.argv[1]
  except IndexError:
    sys.stdout.write("Usage: python %s PIPES_EXECUTABLE\n" % argv[0])
    sys.exit(2)
  run_map(pipes_exe)
  run_reduce(pipes_exe)


if __name__ == '__main__':
  word_count_example(sys.argv)
