# -*- coding: utf-8 -*-
"""
Created on Thu Sep  5 16:58:38 2013

@author: mauro
"""

import pydoop
import os,sys

hadoop_version = pydoop.hadoop_version_info()
if not hadoop_version.is_cloudera():
  sys.exit('Package deb only for cloudera dist, found hadoop %s instead'%pydoop.hadoop_version())

with open(os.path.join(open(os.path.realpath(__file__)), '../VERSION')) as f:
  version = f.read()