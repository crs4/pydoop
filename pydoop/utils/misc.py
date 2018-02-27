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

"""
Miscellaneous utilities.
"""

import logging
import time
import uuid


DEFAULT_LOG_LEVEL = "WARNING"


class NullHandler(logging.Handler):
    def emit(self, record):
        pass


class NullLogger(logging.Logger):
    def __init__(self):
        logging.Logger.__init__(self, "null")
        self.propagate = 0
        self.handlers = [NullHandler()]


def make_random_str(prefix="pydoop_", postfix=''):
    return "%s%s%s" % (prefix, uuid.uuid4().hex, postfix)


class Timer(object):

    def __init__(self, ctx, counter_group=None):
        self.ctx = ctx
        self._start_times = {}
        self._counters = {}
        self._counter_group = counter_group if counter_group else "Timer"

    def _gen_counter_name(self, event):
        return "TIME_" + event.upper() + " (ms)"

    def _get_time_counter(self, name):
        if name not in self._counters:
            counter_name = self._gen_counter_name(name)
            self._counters[name] = self.ctx.get_counter(
                self._counter_group, counter_name
            )
        return self._counters[name]

    def start(self, s):
        self._start_times[s] = time.time()

    def stop(self, s):
        delta_ms = 1000 * (time.time() - self._start_times[s])
        self.ctx.increment_counter(self._get_time_counter(s), int(delta_ms))

    def time_block(self, event_name):
        return self.TimingBlock(self, event_name)

    class TimingBlock(object):

        def __init__(self, timer, event_name):
            self._timer = timer
            self._event_name = event_name

        def __enter__(self):
            self._timer.start(self._event_name)
            return self._timer

        def __exit__(self, exception_type, exception_val, exception_tb):
            self._timer.stop(self._event_name)
            return False
