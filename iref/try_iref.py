# BEGIN_COPYRIGHT
# 
# Copyright 2012 CRS4.
# 
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
# 
# END_COPYRIGHT

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
