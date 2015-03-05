import sys

from avro.io import DatumReader
from avro.datafile import DataFileReader


def main(argv):
    fn = argv[1]
    out_fn = argv[2]
    try:
        avro_mode = argv[3]
    except IndexError:
        avro_mode = ''  # KV is the only special case
    with open(out_fn, 'w') as fo:
        with open(fn, 'rb') as f:
            reader = DataFileReader(f, DatumReader())
            for r in reader:
                if avro_mode.upper() == 'KV':
                    r = r['key']
                fo.write('%s\t%r\n' % (r['office'], r['counts']))
    print 'wrote', out_fn


if __name__ == '__main__':
    main(sys.argv)
