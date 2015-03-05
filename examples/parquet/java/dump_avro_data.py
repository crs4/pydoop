import sys
import os
from cStringIO import StringIO

import avro.schema
import avro.io

import pydoop.hdfs as hdfs


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
SCHEMA_FN = os.path.join(THIS_DIR, '../schemas/user.avsc')


def main(argv):
    try:
        hdfs_avro_dir = argv[1]
    except IndexError:
        sys.exit('Usage: %s HDFS_AVRO_DIR' % argv[0])
    with open(SCHEMA_FN) as f:
        schema = avro.schema.parse(f.read())
    dr = avro.io.DatumReader(schema)
    avro_fnames = [_ for _ in hdfs.ls(hdfs_avro_dir)
                   if hdfs.path.basename(_).startswith('part')]
    for fn in avro_fnames:
        data = hdfs.load(fn)
        f = StringIO(data)
        dec = avro.io.BinaryDecoder(f)
        while f.tell() < len(data):
            u = dr.read(dec)
            print ';'.join(u[_] for _ in ('name', 'office', 'favorite_color'))
            f.read(1)  # skip the newline inserted by TextOutputFormat


if __name__ == '__main__':
    main(sys.argv)
