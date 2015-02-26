import sys
import csv

import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter


FIELDS = ['name', 'office', 'favorite_color']


def main(argv):
    try:
        schema_fn = argv[1]
        csv_fn = argv[2]
        avro_fn = argv[3]
    except IndexError:
        sys.exit('Usage: %s SCHEMA_FILE CSV_FILE AVRO_FILE' % argv[0])
    with open(schema_fn) as f_in:
        schema = avro.schema.parse(f_in.read())
    with open(csv_fn) as f_in:
        reader = csv.reader(f_in, delimiter=';')
        with open(avro_fn, 'wb') as f_out:
            writer = DataFileWriter(f_out, DatumWriter(), schema)
            for row in reader:
                writer.append(dict(zip(FIELDS, row)))
            writer.close()


if __name__ == '__main__':
    main(sys.argv)
