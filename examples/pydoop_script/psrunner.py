# BEGIN_COPYRIGHT
# 
# Copyright 2012 CRS4.
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
Support module for running pydoop script examples.
"""

import subprocess as sp
import pydoop.test_support as pts


PYDOOP_EXE = "../../scripts/pydoop"


class PydoopScriptRunner(pts.PipesRunner):

  BASE_ARGS = [PYDOOP_EXE, "script"]

  def set_input(self, orig_input):
    super(PydoopScriptRunner, self).set_input("", orig_input)

  def run_script(self, script, more_args=None):
    args = self.BASE_ARGS + [script, self.input, self.output]
    self.logger.info("running pydoop script")
    retcode = sp.call(args + (more_args or []))
    if retcode:
      raise RuntimeError("Error running pydoop_script")
