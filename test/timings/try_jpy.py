from timer import Timer

import jpyutil
jvm = jpyutil.init_jvm(jvm_maxmem='48M')

import jpy

N = 256
z = '\xff' * N * 1024
with Timer() as t:
    bz = bytearray(z) # view string as bytearray
    a = jpy.array('byte', bz)  # project this to a java array
    ba = bytearray(a) # bring it back to a python bytearray
print "=> round trip for size %sKB: %ss [%s KB/sec]" % (N, t.secs, N/t.secs)



