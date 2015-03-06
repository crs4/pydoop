import sys
import random

import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter

NAME_POOL = ['george', 'john', 'paul', 'ringo']
OFFICE_POOL = ['office-%d' % _ for _ in xrange(4)]
COLOR_POOL = ['black', 'cyan', 'magenta', 'yellow']


def main(argv):
    try:
        schema_fn = argv[1]
        n_users = int(argv[2])
        avro_fn = argv[3]
    except IndexError:
        sys.exit('Usage: %s SCHEMA_FILE N_USERS AVRO_FILE' % argv[0])
    with open(schema_fn) as f_in:
        schema = avro.schema.parse(f_in.read())
    with open(avro_fn, 'wb') as f_out:
        writer = DataFileWriter(f_out, DatumWriter(), schema)
        for i in xrange(n_users):
            writer.append({
                'name': random.choice(NAME_POOL),
                'office': random.choice(OFFICE_POOL),
                'favorite_color': random.choice(COLOR_POOL),
                'favorite_number': i,
            })
        writer.close()


if __name__ == '__main__':
    main(sys.argv)
