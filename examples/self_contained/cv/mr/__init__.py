# BEGIN_COPYRIGHT
# END_COPYRIGHT
"""
Contains MapReduce components for the cv application.
"""

from pydoop.pipes import runTask, Factory
from mapper import Mapper
from reducer import Reducer


def run_task():
  return runTask(Factory(Mapper, Reducer, combiner_class=Reducer))
