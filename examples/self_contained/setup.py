# BEGIN_COPYRIGHT
# 
# Copyright 2009-2013 CRS4.
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

from distutils.core import setup
import cv


setup(
    name="cv",
    version=cv.__version__,
    description="MR app for counting occurrence of each vowel in input text",
    author=cv.__author__,
    author_email=cv.__author_email__,
    url=cv.__url__,
    packages=["cv", "cv.lib", "cv.mr"]
    )
