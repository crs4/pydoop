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
  
  In Hadoop <= 0.20.2, FileSplit has the following format::
  
      16 bit filename byte length
      filename in bytes
      64 bit offset
      64 bit length
  
  Starting from release 0.21.0, the first field is a variable length
  compressed long. For details, see::

      mapred/src/java/org/apache/hadoop/mapreduce/lib/input/FileSplit.Java
      --> readFields
      common/src/java/org/apache/hadoop/io/Text.java
      --> readString
      common/src/java/org/apache/hadoop/io/WritableUtils.java
      --> readVInt
  
  in Hadoop's source code.
  """
  def __init__(self, data):
    o = 2 + unpack(">h", data[:2])[0]
    self.filename = data[2:o]
    self.offset = unpack(">q", data[o:o+8])[0]
    self.length = unpack(">q", data[o+8:o+16])[0]
