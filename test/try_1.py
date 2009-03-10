import try_m

class Derived(try_m.Base):
  def __init__(self, i, x, s):
    try_m.Base.__init__(self)
    self.i = i
    self.x = x
    self.s = s
  def f(self):
    return self.i
  def g(self):
    return self.x
  def h(self):
    return self.s
  def sref(self):
    return self.s

d = Derived(10, 0.333, 'hekllo')
print d.f()

print try_m.check_base_int(d)
print try_m.check_base_float(d)
print try_m.check_base_string(d)
print try_m.check_base_sref(d)
