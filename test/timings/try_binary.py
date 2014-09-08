import sys
sys.path.insert(0, '../../build/lib.linux-x86_64-2.7')

from pydoop.mapreduce.binary_streams import  BinaryDownStreamFilter, BinaryWriter

from timer import Timer

def write_data(N, fname):
    with open(fname, 'w') as f:
        writer = BinaryWriter(f)
        for i in range(N):
            writer.send('mapItem', "key", "val")
        writer.send('close')


def read_data(fname, N=None):
    with open(fname, 'rb', buffering=(4096*4)) as f:
        reader = BinaryDownStreamFilter(f)
        if N is None:
            for cmd, args in reader:
                pass
        else:
            for i in range(N):
                cmd, args = reader.next()
        
def main():
    fname = 'foo.dat'
    N = 100000
    with Timer() as t:
        write_data(N, fname)
    print "=> write_data: %s s" % t.secs
    with Timer() as t:
        read_data(fname)
    print "=> read_data: %s s" % t.secs
    with Timer() as t:
        read_data(fname, 100000)
    print "=> read_data(100000): %s s" % t.secs
    with Timer() as t:
        read_data(fname, 50000)
    print "=> read_data(50000): %s s" % t.secs

    with open(fname, 'rb', buffering=(4096*4)) as f:
        reader = BinaryDownStreamFilter(f)
        for i in range(10):
            cmd, args = reader.next()
            print cmd, args
main()
