import try_m

at = try_m.a_thing(29020)
ab = try_m.a_box(at)
print ab.f()

class avd(try_m.a_v_thing):
  def __init__(self, x, y='barfoo'):
    try_m.a_v_thing.__init__(self)
    self.x = x
    self.y = y
  def f(self):
    return self.x
  def g(self):
    return self.y

x = avd(10202, 'ehhehe')
print x.f()
print x.g()

ab2 = try_m.a_box(x)

print ab2.f()


class factory(try_m.factory):
  def __init__(self, product_class):
    try_m.factory.__init__(self)
    self.product_class = product_class
    self.produced = []
  def create(self, v):
    o = self.product_class(v, 'barfoo-%d' % v)
    self.produced.append(o)
    return o

foo = factory(avd)
p = foo.create(2333)
print p.f()
try_m.call_factory(foo, 10)


