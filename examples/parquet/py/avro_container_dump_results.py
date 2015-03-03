import sys

from avro.io import DatumReader
from avro.datafile import DataFileReader


def main(argv):
    fn = argv[1]
    out_fn = argv[2]
    with open(out_fn, 'w') as fo:
        with open(fn, 'rb') as f:
            reader = DataFileReader(f, DatumReader())
            for r in reader:
                fo.write('%s\t%r\n' % (r['office'], r['counts']))
    print 'wrote', out_fn


if __name__ == '__main__':
    main(sys.argv)
