from cStringIO import StringIO

from avro.io import DatumWriter, BinaryEncoder


class AvroSerializer(object):

    def __init__(self, schema):
        self.schema = schema
        self.datum_writer = DatumWriter(schema)

    def serialize(self, record):
        f = StringIO()
        encoder = BinaryEncoder(f)
        self.datum_writer.write(record, encoder)
        return f.getvalue()


def avro_user_record(i):
    return {
        "office": 'office-%s' % i,
        "favorite_number": i,
        "favorite_color": 'color-%s' % i,
        "name": 'name-%s' % i,
    }
