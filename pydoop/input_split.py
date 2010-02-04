# BEGIN_COPYRIGHT
# END_COPYRIGHT
"""
Wrapper module for the InputSplit class.
"""

from struct import unpack


class InputSplit(object):
  """
  Represents the data to be processed by an individual C{Mapper}.

  Typically, it presents a byte-oriented view on the input and is
  the responsibility of RecordReader of the job to process this and
  present a record-oriented view. It is created from C{FileSplit} data.
  """
  def __init__(self, data):
    """
    @param data: a byte string in the format::
      <16 bit filename byte length>
      <filename in bytes>
      <64 bit offset>
      <64 bit length>

    U{http://mail-archives.apache.org/mod_mbox/hadoop-core-user/200906.mbox/
    %3C480765DF-B33D-4189-A000-6F51D30CBACB@apache.org%3E}
    """
    o = 2 + unpack(">h", data[:2])[0]
    self.filename = data[2:o]
    self.offset = unpack(">q", data[o:o+8])[0]
    self.length = unpack(">q", data[o+8:o+16])[0]
