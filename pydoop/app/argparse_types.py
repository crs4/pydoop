# BEGIN_COPYRIGHT
#
# Copyright 2009-2019 CRS4.
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

import argparse
import pydoop.hdfs as hdfs


def kv_pair(s):
    try:
        k, v = s.split("=", 1)
    except ValueError:
        raise argparse.ArgumentTypeError("arg must be in the k=v form")
    return k, v


class UpdateMap(argparse.Action):
    """\
    Update the destination map with a K=V pair.

    >>> parser = argparse.ArgumentParser()
    >>> _ = parser.add_argument("-D", metavar="K=V", action=UpdateMap)
    >>> args = parser.parse_args(["-D", "k1=v1", "-D", "k2=v2", "-D", "k2=v3"])
    >>> args.D == {'k1': 'v1', 'k2': 'v3'}
    True
    """

    def __init__(self, option_strings, dest, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if k in {"help", "metavar"}}
        kwargs["type"] = kv_pair
        super(UpdateMap, self).__init__(option_strings, dest, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        if getattr(namespace, self.dest, None) is None:
            setattr(namespace, self.dest, {})
        getattr(namespace, self.dest).update([values])


def a_file_that_can_be_read(x):
    with open(x, 'r'):
        pass
    return x


def a_hdfs_file(x):
    _, _, _ = hdfs.path.split(x)
    return x


def a_comma_separated_list(x):
    # FIXME unclear how does one check for bad lists...
    return x
