# BEGIN_COPYRIGHT
#
# Copyright 2009-2015 CRS4.
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
This module allows you to write the components of your MapReduce application.

The basic MapReduce components (Mapper, Reducer, RecordReader, etc.)
are provided as abstract classes. Application developers must subclass
them, providing implementations for all methods called by the
framework.

**NOTE:** this module is provided only for backward compatibility support.
If you are writing new code, use the api defined in `pydoop.mapreduce.api`
"""

__all__ = ["Mapper", "Reducer", "RecordWriter", "Partitioner", "new_RR",
           "InputSplit", "RecordReaderWrapper", "Factory", "runTask"]

from pydoop.mapreduce.api import (
    Mapper, Reducer, RecordWriter, Partitioner, RecordReader as new_RR
)
from pydoop.mapreduce.pipes import (
    InputSplit, RecordReaderWrapper, Factory, runTask
)


class RecordReader(new_RR):
    """
    Breaks the data into key/value pairs for input to the :class:`Mapper`.
    """
    # we have to override next to document the different return value
    def next(self):
        """
        Called by the framework to provide a key/value pair to the
        :class:`Mapper`.  Applications must override this.

        :rtype: tuple
        :return: a tuple of three elements. The first one is a bool which
        is True if a record is being read and False otherwise (signaling
        the end of the input split). The second and third element are,
        respectively, the key and the value (as strings).
        """
        raise NotImplementedError

    def __iter__(self):
        return RecordReaderWrapper(self)

    def get_progress(self):
        return self.getProgress()

    def getProgress(self):
        raise NotImplementedError


class Combiner(Reducer):
    """
    Works exactly as a :class:`Reducer`, but values aggregation is performed
    locally to the machine hosting each map task.
    """
