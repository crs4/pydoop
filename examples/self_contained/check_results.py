# BEGIN_COPYRIGHT
#
# Copyright 2009-2018 CRS4.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# END_COPYRIGHT

import sys
import re
import logging
from collections import Counter

logging.basicConfig(level=logging.INFO)

from pydoop.hdfs import hdfs
import pydoop.test_support as pts
import pydoop.hadut as hadut


def compute_vc(input_dir):
    fs = hdfs()
    data = []
    for x in fs.list_directory(input_dir):
        with fs.open_file(x['path'], 'rt') as f:
            data.append(f.read())
    all_data = ''.join(data)
    vowels = re.findall('[AEIOUY]', all_data.upper())
    return Counter(vowels)


def get_res(output_dir):
    return pts.parse_mr_output(hadut.collect_output(output_dir), vtype=int)


def check(measured_res, expected_res):
    res = pts.compare_counts(measured_res, expected_res)
    if res:
        return "ERROR: %s" % res
    else:
        return "OK."


def main(argv):
    logger = logging.getLogger("main")
    logger.setLevel(logging.INFO)
    input_dir = argv[1]
    output_dir = argv[2]
    logger.info("checking results")
    measured_res = get_res(output_dir)
    expected_res = compute_vc(input_dir)
    logger.info(check(measured_res, expected_res))


if __name__ == "__main__":
    main(sys.argv)
