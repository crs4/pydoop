import sys
from collections import Counter


def main():
    efname = sys.argv[1]
    rfname = sys.argv[2]

    expected = {}
    with open(efname) as f:
        for l in f:
            p = l.strip().split(';')
            expected.setdefault(p[1], Counter())[p[2]] += 1

    computed = {}
    with open(rfname) as f:
        for l in f:
            p = l.strip().split('\t')
            computed[p[0]] = eval(p[1])

    print 'All is ok!' if expected == computed else 'Something is broken...'


if __name__ == '__main__':
    main()
