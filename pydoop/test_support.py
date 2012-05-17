# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""
Miscellaneous utilities for testing.
"""

import sys, os, uuid
import hdfs


def inject_code(new_code, target_code):
  new_code = "{0}#---AUTO-INJECTED---{0}{1}{0}#-------------------{0}".format(
    os.linesep, os.linesep.join(new_code.strip().splitlines())
    )
  if target_code.startswith("#!"):
    target_code = target_code.splitlines()
    target_code[1:1] = [new_code]
    target_code = os.linesep.join(target_code)
  else:
    target_code = new_code + target_code
  return target_code


def add_sys_path(target_code):
  new_code = os.linesep.join([
    "import sys",
    "sys.path = %r" % (sys.path,)
    ])
  return inject_code(new_code, target_code)


def make_random_str(prefix="pydoop_test_"):
  return "%s%s" % (prefix, uuid.uuid4().hex)


def collect_output(mr_out_dir):
  output = []
  for fn in hdfs.ls(mr_out_dir):
    if hdfs.path.basename(fn).startswith("part"):
      with hdfs.open(fn) as f:
        output.append(f.read())
  return "".join(output)


class LocalWordCount(object):

  def __init__(self, input_dir):
    self.input_dir = input_dir
    self.__expected_output = None

  @property
  def expected_output(self):
    if self.__expected_output is None:
      self.__expected_output = self.run()
    return self.__expected_output

  def run(self):
    wc = {}
    for fn in os.listdir(self.input_dir):
      if fn[0] == ".":
        continue
      with open(os.path.join(self.input_dir, fn)) as f:
        for line in f:
          line = line.split()
          for w in line:
            wc[w] = wc.get(w, 0) + 1
    return wc

  def check(self, output):
    res = LocalWordCount.__compare_counts(
      self.__parse_mr_output(output), self.expected_output
      )
    if res:
      return "ERROR: %s" % res
    else:
      return "OK."

  @staticmethod
  def __parse_mr_output(output):
    wc = {}
    for line in output.splitlines():
      if line.isspace():
        continue
      try:
        w, c = line.split()
        c = int(c)
      except (ValueError, TypeError):
        raise ValueError("bad output format")
      wc[w] = c
    return wc

  @staticmethod
  def __compare_counts(c1, c2):
    if len(c1) != len(c2):
      print len(c1), len(c2)
      return "number of keys differs"
    keys = sorted(c1)
    if sorted(c2) != keys:
      return "key lists are different"
    for k in keys:
      if c1[k] != c2[k]:
        return "values are different for key %r (%r != %r)" % (k, c1[k], c2[k])
