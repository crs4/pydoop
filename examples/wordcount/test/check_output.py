#!/usr/bin/python

import sys, os


def wc_from_input(input_dir):
  wc = {}
  for fn in os.listdir(input_dir):
    if fn[0] == ".":
      continue
    f = open(os.path.join(input_dir, fn))
    for line in f:
      line = line.split()
      for w in line:
        wc[w] = wc.get(w, 0) + 1
    f.close()
  return wc


def wc_from_output(output_dir):
  wc = {}
  for fn in os.listdir(output_dir):
    if fn[0] == "." or fn[0] == "_":
      continue
    f = open(os.path.join(output_dir, fn))
    for line in f:
      try:
        w, c = line.split()
        c = int(c)
      except (ValueError, TypeError):
        raise ValueError("%r: bad output format" % f.name)
      wc[w] = c
    f.close()
  return wc


def dict_equal(d1, d2):
  if len(d1) != len(d2):
    print len(d1), len(d2)
    return "number of keys differs"
  keys = sorted(d1)
  if sorted(d2) != keys:
    return "key lists are different"
  for k in keys:
    if d1[k] != d2[k]:
      return "values are different for key %r (%r != %r)" % (k, d1[k], d2[k])
  return "OK."


def main(argv):
  try:
    input_dir = argv[1]
    output_dir = argv[2]
  except IndexError:
    print "Usage: python %s INPUT_DIR OUTPUT_DIR" % argv[0]
    sys.exit(2)

  print dict_equal(wc_from_input(input_dir), wc_from_output(output_dir))


if __name__ == "__main__":
  main(sys.argv)
