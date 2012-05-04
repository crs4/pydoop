# BEGIN_COPYRIGHT
# END_COPYRIGHT

import os

MB = 2**20
TEST_ROOT = os.getenv("TEST_ROOT", "pydoop_test_tree")

def isdir(fs, d):
  try:
    info = fs.get_path_info(d)
  except IOError:
    return False
  return info['kind'] == 'directory'
