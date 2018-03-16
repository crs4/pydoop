# BEGIN_COPYRIGHT
#
# Copyright 2009-2018 CRS4.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# END_COPYRIGHT

import random
import sys

offices = ['office-%s' % i for i in range(3)]
colors = ['red', 'blue', 'yellow', 'orange', 'maroon', 'green']
names = ['Alyssa', 'John', 'Kathy', 'Ben', 'Karla', 'Ross', 'Violetta']


def create_input(n, stream):
    for i in range(n):
        stream.write(';'.join([
            random.choice(names),
            random.choice(offices),
            random.choice(colors),
        ]) + '\n')


def main(n, filename):
    with open(filename, 'w') as f:
        create_input(n, f)


if __name__ == '__main__':
    main(int(sys.argv[1]), sys.argv[2])
