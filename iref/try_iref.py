import sys, gc
import iref


class py_wrap_payload(iref.payload):
  def __init__(self, v):
    sys.stderr.write("py_wrap_payload::__init__(%d)\n" % v)
    iref.payload.__init__(self, v)
  def __del__(self):
    sys.stderr.write("py_wrap_payload::__del__ ()\n")


def payload_maker(x):
  return py_wrap_payload(x)


def payload_destructor(x):
  del x


def emulate_payload_user(f):
  sys.stderr.write("emulate_payload_user:: -- 0 --\n")
  pl = f(17)
  sys.stderr.write("emulate_payload_user:: -- 1 --\n")
  payload_destructor(pl)
  sys.stderr.write("emulate_payload_user:: -- 2 --\n")


def main():
  a = py_wrap_payload(33)
  print 'a.get() = ', a.get()
  gc.collect()
  sys.stderr.write("+++emulate_payload_user+++\n")
  emulate_payload_user(payload_maker)
  sys.stderr.write("+++iref.payload_user+++\n")
  iref.payload_user(payload_maker)
  sys.stderr.write("+++garbage collection+++\n")
  gc.collect()
  sys.stderr.write("+++end of test+++\n")


if __name__ == "__main__":
  main()
