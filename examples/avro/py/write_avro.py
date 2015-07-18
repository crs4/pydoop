import sys
import csv

import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter


FIELDS = ['name', 'office', 'favorite_color']


def main(schema_fn, csv_fn, avro_fn):

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
    try:
        schema_fn = sys.argv[1]
        csv_fn = sys.argv[2]
        avro_fn = sys.argv[3]
    except IndexError:
        sys.exit('Usage: %s SCHEMA_FILE CSV_FILE AVRO_FILE' % sys.argv[0])
    main(schema_fn, csv_fn, avro_fn)
