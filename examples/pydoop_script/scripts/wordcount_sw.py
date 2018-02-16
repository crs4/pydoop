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

"""\
Word count with stop words (i.e., words that should be ignored).
"""

# DOCS_INCLUDE_START
STOP_WORDS_FN = 'stop_words.txt'

try:
    with open(STOP_WORDS_FN) as f:
        STOP_WORDS = frozenset(l.strip() for l in f if not l.isspace())
except OSError as e:
    STOP_WORDS = frozenset()


def mapper(_, value, writer):
    for word in value.split():
        if word in STOP_WORDS:
            writer.count("STOP_WORDS", 1)
        else:
            writer.emit(word, 1)


def reducer(word, icounts, writer):
    writer.emit(word, sum(icounts))
