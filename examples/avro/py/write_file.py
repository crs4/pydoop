import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import random

schema = avro.schema.parse(open("../schemas/user.avsc").read())

writer = DataFileWriter(open("users.avro", "w"), 
                        DatumWriter(), schema)

N = 200
offices = ['office-%s' % i for i in xrange(3)]
colors  = ['red', 'blue', 'yellow', 'orange', 'maroon', 'green']
names = ['Alyssa', 'John', 'Kathy', 'Ben', 'Karla', 'Ross', 'Violetta']

for i in xrange(N):
    writer.append({"office": random.choice(offices),
                   "favorite_number": random.choice(range(10)),
                   "favorite_color":  random.choice(colors),
                   "name" : random.choice(names)})
writer.close()

# reader = DataFileReader(open("users.avro", "r"), DatumReader())
# for user in reader:
#     print user
# reader.close()
