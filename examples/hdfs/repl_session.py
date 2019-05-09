"""\
# DOCS_INCLUDE_START
>>> import pydoop.hdfs as hdfs
>>> hdfs.mkdir('test')
>>> hdfs.dump('hello, world', 'test/hello.txt')
>>> hdfs.load('test/hello.txt')
b'hello, world'
>>> hdfs.load('test/hello.txt', mode='rt')
'hello, world'
>>> [hdfs.path.basename(_) for _ in hdfs.ls('test')]
['hello.txt']
>>> hdfs.stat('test/hello.txt').st_size
12
>>> hdfs.path.isdir('test')
True
>>> hdfs.path.isfile('test')
False
>>> hdfs.path.basename('test/hello.txt')
'hello.txt'
>>> hdfs.cp('test', 'test.copy')
>>> [hdfs.path.basename(_) for _ in hdfs.ls('test.copy')]
['hello.txt']
>>> hdfs.get('test/hello.txt', '/tmp/hello.txt')
>>> with open('/tmp/hello.txt') as f:
...     f.read()
...
'hello, world'
>>> hdfs.put('/tmp/hello.txt', 'test.copy/hello.txt.copy')
>>> for x in sorted(hdfs.ls('test.copy')): print(repr(hdfs.path.basename(x)))
...
'hello.txt'
'hello.txt.copy'
>>> with hdfs.open('test/hello.txt', 'r') as fi:
...     fi.read(3)
...
b'hel'
>>> with hdfs.open('test/hello.txt', 'rt') as fi:
...     fi.read(3)
...
'hel'

# DOCS_INCLUDE_END
"""


def clean():
    for path in "test", "test.copy", "file:/tmp/hello.txt":
        try:
            hdfs.rm(path)
        except OSError:
            pass


if __name__ == "__main__":
    import doctest
    import pydoop.hdfs as hdfs
    clean()
    doctest.testmod(verbose=True)
    clean()
