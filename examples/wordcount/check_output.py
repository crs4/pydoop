# BEGIN_COPYRIGHT
# END_COPYRIGHT

import os


def local_wc(input_dir):
  wc = {}
  for fn in os.listdir(input_dir):
    if fn[0] == ".":
      continue
    with open(os.path.join(input_dir, fn)) as f:
      for line in f:
        line = line.split()
        for w in line:
          wc[w] = wc.get(w, 0) + 1
  return wc


def parse_output(output):
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


def compare_counts(c1, c2):
  if len(c1) != len(c2):
    print len(c1), len(c2)
    return "number of keys differs"
  keys = sorted(c1)
  if sorted(c2) != keys:
    return "key lists are different"
  for k in keys:
    if c1[k] != c2[k]:
      return "values are different for key %r (%r != %r)" % (k, c1[k], c2[k])


def check(input_dir, output):
  expected_output = local_wc(input_dir)
  res = compare_counts(parse_output(output), expected_output)
  if res:
    return "ERROR: %s" % res
  else:
    return "OK."
