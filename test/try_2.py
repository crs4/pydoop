import try_m

class Derived(try_m.A):
  def __init__(self, x):
    try_m.A.__init__(self)
    self.x = x
  def getName(self):
    return self.x

d = Derived('hekllo')
print d.getName()

d.internal_check()


