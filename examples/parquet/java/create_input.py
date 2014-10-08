import random
import sys

offices = ['office-%s' % i for i in xrange(3)]
colors  = ['red', 'blue', 'yellow', 'orange', 'maroon', 'green']
names = ['Alyssa', 'John', 'Kathy', 'Ben', 'Karla', 'Ross', 'Violetta']


def create_input(n, stream):
    for i in range(n):
        stream.write(';'.join([
            random.choice(names),
            random.choice(offices),
            random.choice(colors),
        ]) + '\n')


def main(argv):
    with open(argv[2], 'w') as f:
        create_input(int(argv[1]), f)

main(sys.argv)