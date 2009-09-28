
# On Tue, Jun 16, 2009 at 12:22 PM, Owen O'Malley <omal...@apache.org> wrote:
# > Sorry, I forget how much isn't clear to people who are just starting.
# >
# > FileInputFormat creates FileSplits. The serialization is very stable and
# > can't be changed without breaking things. The reason that pipes can't
# > stringify it is that the string form of input splits are ambiguous (and
# > since it is user code, we really can't make assumptions about it). The
# > format of FileSplit is:
# >
# > <16 bit filename byte length>
# > <filename in bytes>
# > <64 bit offset>
# > <64 bit length>
# >
# > Technically the filename uses a funky utf-8 encoding, but in practice as
# > long as the filename has ascii characters they are ascii. Look at
# > org.apache.hadoop.io.UTF.writeString for the precise definition.


from struct import unpack

class InputSplit(object):
  def __init__(self, data):
    o = 2 + unpack(">h", data[:2])[0]
    self.filename = data[2:o]
    self.offset = unpack(">q", data[o:o+8])[0]
    self.length = unpack(">q", data[o+8:o+16])[0]

if __name__ == '__main__':
  data = '\x00/hdfs://localhost:9000/user/zag/in-dir/FGCS-1.ps\x00\x00\x00\x00\x00\x08h(\x00\x00\x00\x00\x00\x08h\x05'
  i = InputSplit(data)
  print i.filename
  print i.offset
  print i.length



