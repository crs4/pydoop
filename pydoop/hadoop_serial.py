from struct import pack

def serialize_int(i, stream):
  serialize_long(i, stream)

def serialize_long(i, stream):
  if i >= -112 and i <= 127:
    stream.write(pack("B", i))
    return
  length = -112
  if i < 0:
    i ^= -111
    length = -120
  tmp = i
  while tmp != 0:
    tmp >>= 8
    length -= 1
  stream.write(pack("B", length))
  length = - (length + 120) if length < -120 else - (length + 112)

