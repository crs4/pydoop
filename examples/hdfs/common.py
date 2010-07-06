# BEGIN_COPYRIGHT
# END_COPYRIGHT

MB = 2**20
HOSTNAME = "localhost"
PORT = 9000
TEST_ROOT = "tree_test"

def isdir(fs, d):
  try:
    info = fs.get_path_info(d)
  except IOError:
    return False
  return info['kind'] == 'directory'
