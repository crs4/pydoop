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
