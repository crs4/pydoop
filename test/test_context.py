import pydoop_core


jc_fields = {'foo' : 'a-string', 'bar' : 32}

class jc(pydoop_core.JobConf):
  def __init__(self, d):
    pydoop_core.JobConf.__init__(self)
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


class tc(pydoop_core.TaskContext):
  def __init__(self, j=None):
    pydoop_core.TaskContext.__init__(self)
    self.job_conf = j
    for k in jc_fields.keys():
      print '%s -> %s' % (k, self.job_conf.get(k))
  def getInputKey(self):
    return 'tc.getInputKey()'
  def getInputValue(self):
    return 'tc.getInputValue()'
  def emit(self, k, v):
    print 'emitting %s -> %s' % (k,v)
  def getJobConf(self):
    return self.job_conf

class mc(pydoop_core.MapContext):
  def __init__(self, jc):
    pydoop_core.MapContext.__init__(self)
    self.job_conf = jc
  def getInputKey(self):
    return 'mc.getInputKey()'
  def getInputValue(self):
    return 'mc.getInputValue()'
  def emit(self, k, v):
    print 'emitting %s -> %s' % (k,v)

class rc(pydoop_core.ReduceContext):
  def __init__(self, jc):
    pydoop_core.ReduceContext.__init__(self)
    self.job_conf = jc
  def getInputKey(self):
    return 'rc.getInputKey()'
  def getInputValue(self):
    return 'rc.getInputValue()'
  def emit(self, k, v):
    print 'emitting %s -> %s' % (k,v)


if __name__ == '__main__':
  j = jc({'foo' : 'a-string', 'bar' : 32})
  t = tc(j)
  pydoop_core.try_context(t)
  m = mc(j)
  pydoop_core.try_context(m)
  r = rc(j)
  pydoop_core.try_context(r)





