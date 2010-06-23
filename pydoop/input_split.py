# BEGIN_COPYRIGHT
# END_COPYRIGHT

from struct import unpack


class InputSplit(object):
  """
  Represents the data to be processed by an individual :class:`Mapper`\ .

  Typically, it presents a byte-oriented view on the input and it is
  the responsibility of the :class:`RecordReader` to convert this to a
  record-oriented view.
  
  :param data: a byte string, the FileSplit
  :type data: string
  
  The FileSplit has the following format::
    
      16 bit filename byte length
      filename in bytes
      64 bit offset
      64 bit length

  Source: `FileSplit format description by Owen O'Malley <http://mail-archives.apache.org/mod_mbox/hadoop-core-user/200906.mbox/%3c480765DF-B33D-4189-A000-6F51D30CBACB@apache.org%3e>`_\ .

  """
  def __init__(self, data):
    o = 2 + unpack(">h", data[:2])[0]
    self.filename = data[2:o]
    self.offset = unpack(">q", data[o:o+8])[0]
    self.length = unpack(">q", data[o+8:o+16])[0]
