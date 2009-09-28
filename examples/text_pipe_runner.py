from pydoop.utils import pipes_runner

class output_visitor(object):
  def __init__(self):
    pass
  def output(self, key, value):
    print 'output pair -> <%s,%s>' % (key, value)
  def done(self):
    print 'all done'
  def progress(self, progress):
    print 'we are at %f done.' % float(progress)

def run_map():
  p = pipes_runner('./WordCount', output_visitor())
  p.start()
  p.run_map('fake_input_split', 3)
  for k in ['faa', 'fii', 'foo', 'fuu']:
    p.map_item(str(1), str(k))
  p.close()

def run_reduce():
  p = pipes_runner('./WordCount', output_visitor())
  p.start()
  p.run_reduce()
  red_vals = {'foo' : range(10), 'bar' : range(14)}
  for k in red_vals.keys():
    p.reduce_key(str(k))
    for v in red_vals[k]:
      p.reduce_value(str(v))
  p.close()

def word_count_example():
  run_map()
  run_reduce()


if __name__ == '__main__':
  word_count_example()





