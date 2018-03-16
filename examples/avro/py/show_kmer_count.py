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
            sys.stdout.write('%s\t%d\n' % t)
            if i + 1 >= LIMIT:
                break


if __name__ == '__main__':
    main(sys.argv)
