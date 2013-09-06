# -*- coding: utf-8 -*-
"""
Created on Thu Sep  5 16:58:38 2013

@author: mauro
"""

import pydoop
import os, sys, re, subprocess, shutil

hadoop_version = pydoop.hadoop_version_info()
if not hadoop_version.is_cloudera():
  sys.exit('Package deb only for cloudera dist, found hadoop %s instead'%pydoop.hadoop_version())
cdh_version = '{0}.{1}.{2}'.format(*pydoop.hadoop_version_info().tuple[-3:])

deb_path = os.path.dirname(os.path.realpath(__file__))
with open(os.path.join(deb_path, '../VERSION'), 'r') as f:
  new_version = f.read().strip()

path_ch = os.path.join(deb_path, '../debian/changelog')
shutil.copyfile('{0}.cdh{1}'.format(path_ch, cdh_version),
  path_ch)

with open(path_ch, 'r') as f:
  last_version = re.search('(\d+\.\d+\.\d+)', f.read()).group(0)

args_dch = [
  "dch",  
  "Packages Pydoop {0} for cdh{1}".format(new_version, cdh_version),
  "-c",
  path_ch          
]
if new_version == last_version:
  args_dch.append("-i")
else:
  args_dch.extend(["--newversion", 
                   "{0}-cdh{1}".format(new_version, cdh_version)
])
subprocess.call(args_dch)