# BEGIN_COPYRIGHT
# END_COPYRIGHT
"""
This is a trivial application that counts the occurence of each vowel
in a text input stream.
"""

from pydoop.pipes import runTask, Factory
from mapper import Mapper
from reducer import Reducer


def run_task():
  return runTask(Factory(Mapper, Reducer, combiner_class=Reducer))
