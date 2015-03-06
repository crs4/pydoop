import sys
import csv
from operator import itemgetter


LIMIT = 10


def main(argv):
    with open(argv[1]) as f:
        reader = csv.reader(f, delimiter='\t')
        data = [(k, int(v)) for (k, v) in reader]
        data.sort(key=itemgetter(1), reverse=True)
        for i, t in enumerate(data):
            print '%s\t%d' % t
            if i+1 >= LIMIT:
                break


if __name__ == '__main__':
    main(sys.argv)
