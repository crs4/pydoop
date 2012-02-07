
# Copyright (C) 2011-2012 CRS4.
#
# This file is part of Seal.
#
# Seal is free software: you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option)
# any later version.
#
# Seal is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
# or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
# for more details.
#
# You should have received a copy of the GNU General Public License along
# with Seal.  If not, see <http://www.gnu.org/licenses/>.


import copy
import os
import sys
import subprocess
import glob

SealJarName = "seal.jar"

def __is_exe(fpath):
	return os.path.exists(fpath) and os.access(fpath, os.X_OK)

def num_nodes():
	hproc = subprocess.Popen([hadoop, "job", "-list-active-trackers"], stdout=subprocess.PIPE)
	stdout, stderr = hproc.communicate()
	if hproc.returncode == 0:
		return stdout.count("\n") # trackers are returned one per line
	else:
		raise RuntimeError("Error running hadoop job -list-active-trackers")

def hdfs_path_exists(path):
	retcode = subprocess.call([hadoop, 'dfs', '-stat', path], stdout=open('/dev/null', 'w'), stderr=subprocess.STDOUT)
	return retcode == 0

def run_hadoop_cmd_e(cmd, properties=None, args_list=[]):
	retcode = run_hadoop_cmd(cmd, properties, args_list)
	if retcode != 0:
		raise RuntimeError("Error running Hadoop command")

def run_hadoop_cmd(cmd, properties=None, args_list=[]):
	args = [hadoop, cmd]
	if properties:
		args += __construct_property_args(properties)
	args += map(str, args_list) # only string arguments are allowed
	return subprocess.call(args)

def dfs(*args):
	return run_hadoop_cmd_e("dfs", args_list=args)

def run_hadoop_jar(jar_name, properties=None, args_list=[]):
	if os.path.exists(jar_name) and os.access(jar_name, os.R_OK):
		args = [hadoop, 'jar', jar_name]
		if properties:
			args += __construct_property_args(properties)
		args += map(str, args_list)
		retcode = subprocess.call(args)
		if retcode != 0:
			raise RuntimeError("Error running hadoop jar")
	else:
		raise ValueError("Can't read jar file %s" % jar_name)

def __construct_property_args(prop_dict):
	return sum(map(lambda pair: ["-D", "%s=%s" % pair], prop_dict.iteritems()), []) # sum flattens the list

def run_class_e(class_name, additional_cp=None, properties=None, args_list=[]):
	retcode = run_class(class_name, additional_cp, properties, args_list)
	if retcode != 0:
		raise RuntimeError("Error running Hadoop class")

def run_pipes(executable, input_path, output_path, properties=None, args_list=[]):
	args = [hadoop, "pipes"]
	properties = properties.copy() if properties else {}
	properties['hadoop.pipes.executable'] = executable

	args.extend( __construct_property_args(properties) )
	args.extend(["-input", input_path, "-output", output_path])
	args.extend(args_list)
	return subprocess.call(args)


def run_class(class_name, additional_cp=None, properties=None, args_list=[]):
	"""
	Run a class that needs the Hadoop jars in its class path
	"""
	args = [hadoop, class_name]
	if additional_cp:
		env = copy.copy(os.environ)
		if type(additional_cp) == str: # wrap a single class path in a list
			additional_cp = [additional_cp]
		env['HADOOP_CLASSPATH'] = ":".join(additional_cp)
	else:
		env = os.environ
	if properties:
		args.extend( __construct_property_args(properties) )
	args.extend(args_list)
	return subprocess.call(args)

def find_jar(jar_name, root_path=None):
	root = root_path or os.getcwd()
	paths = (root, os.path.join(root, "build"), "/usr/share/java")
	for path in [ os.path.join(path, jar_name) for path in paths ]:
		if os.path.exists(path):
			return path
	return None

def find_seal_jar(root_path = None):
	return find_jar(SealJarName, root_path)

#################################################################################
# module initialization
#################################################################################

hadoop = None

if os.environ.has_key("HADOOP_HOME") and \
	__is_exe(os.path.join(os.environ["HADOOP_HOME"], "bin", "hadoop")):
	hadoop = os.path.join(os.environ["HADOOP_HOME"], "bin", "hadoop")
else:
	# search the PATH for hadoop
	for path in os.environ["PATH"].split(os.pathsep):
		hpath = os.path.join(path, 'hadoop')
		if __is_exe(hpath):
			hadoop = hpath
			break
	if hadoop is None:
		raise ImportError("Couldn't find hadoop executable.  Please set HADOOP_HOME or add the hadoop executable to your PATH")
	hadoop = os.path.abspath(hadoop)

