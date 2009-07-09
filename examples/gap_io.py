from struct import pack, unpack, calcsize
from cStringIO import StringIO

class image(object):
  MAGIC_NUMBER    = 'GAP00001'
  N_CHANNELS      = 4
  BYTES_PER_PIXEL = 2
  CHANNELS_ORDER  = 'acgt'
  HEADER_FIELDS   = ['size', 'lane', 'tile', 'cycle', 'xsize', 'ysize']

  @staticmethod
  def image_size(xsize, ysize):
    size_mn = len(image.MAGIC_NUMBER)
    header_size = size_mn + 4*len(image.HEADER_FIELDS)
    channel_size = image.BYTES_PER_PIXEL * xsize * ysize
    return header_size + image.N_CHANNELS * channel_size
  #--
  def _get_block(self, offset, fmt):
    s = calcsize(fmt)
    return offset + s, unpack(fmt, self.data[offset: offset + s])[0]
  #--
  def _get_int(self, offset):
    return self._get_block(offset, "i")
  #--
  def __init__(self, data):
    assert data.startswith(self.MAGIC_NUMBER)
    self.data  = data
    offset = len(self.MAGIC_NUMBER)
    offset, size = self._get_int(offset)
    assert len(data) == size
    offset, self.lane  = self._get_int(offset)
    offset, self.tile  = self._get_int(offset)
    offset, self.cycle = self._get_int(offset)
    offset, self.xsize = self._get_int(offset)
    offset, self.ysize = self._get_int(offset)
    self.channels_data_offset = offset
    self.channel_data_size = self.BYTES_PER_PIXEL * self.xsize * self.ysize
  #--
  def get_channel_data(self, chan):
    beg = self.channels_data_offset + chan * self.channel_data_size
    end = beg + self.channel_data_size
    return self.data[beg:end]

class factory(object):
  def __init__(self, xsize, ysize):
    self.xsize = xsize
    self.ysize = ysize
    size_mn = len(image.MAGIC_NUMBER)
    self.header_size = size_mn + 4*len(image.HEADER_FIELDS)
    self.channel_size = image.BYTES_PER_PIXEL * self.xsize * self.ysize
    self.record_size = self.header_size + image.N_CHANNELS * self.channel_size
    self.sbuff = StringIO()
  #--
  def make(self, lane, tile, cycle, img_a, img_c, img_g, img_t):
    self.sbuff.seek(0)
    self._write_header(lane, tile, cycle)
    self._write_channel(img_a)
    self._write_channel(img_c)
    self._write_channel(img_g)
    self._write_channel(img_t)
    return image(self.sbuff.getvalue())
  #--
  def _write_header(self, lane, tile, cycle):
    size_mn = len(image.MAGIC_NUMBER)
    self.sbuff.write(pack('%ds' % size_mn, image.MAGIC_NUMBER))
    self.sbuff.write(pack('6i', self.record_size,
                          lane, tile, cycle,
                          self.xsize, self.ysize))
  def _write_channel(self, data):
    assert len(data) == self.channel_size
    self.sbuff.write(data)
  #--

#-----------------------------------------------------------------
import random

class fake_factory(factory):
  def __make_rnd_img(self):
    return  ''.join(map(lambda x: chr(random.randint(0, 255)),
                        range(2 * self.xsize * self.ysize)))
  #--
  def make(self, lane, tile, cycle):
    a = self.__make_rnd_img()
    c = self.__make_rnd_img()
    g = self.__make_rnd_img()
    t = self.__make_rnd_img()
    return factory.make(self, lane, tile, cycle, a, c, g, t)


#-----------------------------------------------------------------
from pydoop.hdfs  import hdfs
from pydoop.utils import split_hdfs_path

import os

def generic_open(path, flags='r'):
  hdfs_flags = os.O_RDONLY
  if path.startswith('hdfs:'):
    host, port, fpath = split_hdfs_path(path)
    print 'opening connection to %s:%d -- %s' % (host, port, fpath)
    fs = hdfs(host, port)
    if flags == 'w':
      hdfs_flags = os.O_WRONLY
    return fs.open_file(fpath, hdfs_flags, 0, 0, 0)
  else:
    return open(path, flags)

def main():
  import sys
  file_name = sys.argv[1]
  print file_name

  xsize, ysize = 10, 10
  lane, tile, cycle = 10, 2, 3

  img_a = make_rnd_img(xsize, ysize)
  img_c = make_rnd_img(xsize, ysize)
  img_g = make_rnd_img(xsize, ysize)
  img_t = make_rnd_img(xsize, ysize)

  f = factory(xsize, ysize)
  img_a = make_rnd_img(xsize, ysize)
  r = f.make(lane, tile, cycle, img_a, img_c, img_g, img_t)

  f_out = generic_open(file_name, 'w')
  f_out.write(r.data)
  f_out.close()

  f_in = generic_open(file_name)
  i = image(f_in.read(image.image_size(xsize, ysize)))
  f_in.close()

  assert i.xsize == xsize
  assert i.xsize == xsize
  assert i.tile  == tile
  assert i.cycle == cycle
  assert i.get_channel_data(0) == img_a
  assert i.get_channel_data(1) == img_c
  assert i.get_channel_data(2) == img_g
  assert i.get_channel_data(3) == img_t

if __name__ == '__main__':
  main()
