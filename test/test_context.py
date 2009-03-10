import hadoop_pipes

class jc(hadoop_pipes.JobConf):
  def __init__(self, d):
    hadoop_pipes.JobConf.__init__(self)
    self.d = d
  def hasKey(self, k):
    return self.d.has_key(k)
  def get(self, k):
    return self.d[k]
  def getInt(self, k):
    return int(self.get(k))
  def getFloat(self, k):
    return float(self.get(k))
  def getBoolean(self, k):
    return bool(self.get(k))


class tc(hadoop_pipes.TaskContext):
  def __init__(self, j=None):
    hadoop_pipes.TaskContext.__init__(self)
    self.job_conf = j
  def getInputKey(self):
    return 'tc.getInputKey()'
  def getInputValue(self):
    return 'tc.getInputValue()'
  def emit(self, k, v):
    print 'emitting %s -> %s' % (k,v)
  def getJobConf(self):
    return self.job_conf

class mc(hadoop_pipes.MapContext):
  def __init__(self):
    hadoop_pipes.MapContext.__init__(self)
  def getInputKey(self):
    return 'mc.getInputKey()'
  def getInputValue(self):
    return 'mc.getInputValue()'
  def emit(self, k, v):
    print 'emitting %s -> %s' % (k,v)

class rc(hadoop_pipes.ReduceContext):
  def __init__(self):
    hadoop_pipes.ReduceContext.__init__(self)
  def getInputKey(self):
    return 'rc.getInputKey()'
  def getInputValue(self):
    return 'rc.getInputValue()'
  def emit(self, k, v):
    print 'emitting %s -> %s' % (k,v)


if __name__ == '__main__':
  t = tc()
  hadoop_pipes.try_context(t)

  m = mc()
  hadoop_pipes.try_context(m)

  r = rc()
  hadoop_pipes.try_context(r)


