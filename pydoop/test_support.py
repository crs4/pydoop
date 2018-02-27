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

"""
Miscellaneous utilities for testing.
"""

from __future__ import print_function

import sys
import os
import tempfile

from pydoop.hdfs import default_is_local
from pydoop.utils.py3compat import iteritems


def __inject_pos(code, start=0):
    pos = code.find("import", start)
    if pos < 0:
        return start
    pos = code.rfind(os.linesep, 0, pos) + 1
    endpos = code.find(os.linesep, pos) + 1
    if "__future__" in code[pos:endpos]:
        return __inject_pos(code, endpos)
    else:
        return pos


def inject_code(new_code, target_code):
    """
    Inject new_code into target_code, before the first non-future import.

    NOTE: this is just a hack to make examples work out-of-the-box, in
    the general case it can fail in several ways.
    """
    new_code = "{0}#--AUTO-INJECTED--{0}{1}{0}#-----------------{0}".format(
        os.linesep, os.linesep.join(new_code.strip().splitlines())
    )
    pos = __inject_pos(target_code)
    return target_code[:pos] + new_code + target_code[pos:]


def add_sys_path(target_code):
    new_code = os.linesep.join([
        "import sys",
        "sys.path = %r" % (sys.path,)
    ])
    return inject_code(new_code, target_code)


def set_python_cmd(code, python_cmd=sys.executable):
    python_cmd = python_cmd.strip()
    if not python_cmd.startswith(os.sep):
        python_cmd = os.path.join("", "usr", "bin", "env", python_cmd)
    if code.startswith("#!"):
        pos = code.find(os.linesep, 2)
        code = "" if pos < 0 else code[pos + 1:]
    return "#!%s%s%s" % (python_cmd, os.linesep, code)


def adapt_script(code, python_cmd=sys.executable):
    return set_python_cmd(add_sys_path(code), python_cmd=python_cmd)


def parse_mr_output(output, vtype=str):
    d = {}
    for line in output.splitlines():
        if line.isspace():
            continue
        try:
            k, v = line.split()
            v = vtype(v)
        except (ValueError, TypeError):
            raise ValueError("bad output format")
        if k in d:
            raise ValueError("duplicate key: %r" % (k,))
        d[k] = v
    return d


def compare_counts(c1, c2):
    if len(c1) != len(c2):
        print(len(c1), len(c2))
        return "number of keys differs"
    keys = sorted(c1)
    if sorted(c2) != keys:
        return "key lists are different"
    for k in keys:
        if c1[k] != c2[k]:
            return "values are different for key %r (%r != %r)" % (
                k, c1[k], c2[k]
            )


class LocalWordCount(object):

    def __init__(self, input_path, min_occurrence=0, stop_words=None):
        self.input_path = input_path
        self.min_occurrence = min_occurrence
        self.stop_words = frozenset(stop_words or [])
        self.__expected_output = None

    @property
    def expected_output(self):
        if self.__expected_output is None:
            self.__expected_output = self.run()
        return self.__expected_output

    def run(self):
        wc = {}
        if os.path.isdir(self.input_path):
            for fn in os.listdir(self.input_path):
                if fn[0] == ".":
                    continue
                self._wordcount_file(wc, fn, self.input_path)
        else:
            self._wordcount_file(wc, self.input_path)
        if self.min_occurrence:
            wc = dict(t for t in iteritems(wc) if t[1] >= self.min_occurrence)
        return wc

    def _wordcount_file(self, wc, fn, path=None):
        with open(os.path.join(path, fn) if path else fn) as f:
            for line in f:
                for w in line.split():
                    if w not in self.stop_words:
                        wc[w] = wc.get(w, 0) + 1

    def check(self, output):
        res = compare_counts(
            parse_mr_output(output, vtype=int), self.expected_output
        )
        if res:
            return "ERROR: %s" % res
        else:
            return "OK."


def get_wd_prefix(base="pydoop_"):
    if default_is_local():
        return os.path.join(tempfile.gettempdir(), "pydoop_")
    else:
        return base
