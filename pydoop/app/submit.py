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
An interface to simplify pydoop jobs submission.
"""

import os
import sys
import glob
import argparse
import logging
import uuid
logging.basicConfig(level=logging.INFO)

import pydoop
import pydoop.hdfs as hdfs
import pydoop.hadut as hadut
import pydoop.utils as utils
import pydoop.utils.conversion_tables as conv_tables
from pydoop.mapreduce.pipes import PSTATS_DIR, PSTATS_FMT

from .argparse_types import kv_pair, a_file_that_can_be_read
from .argparse_types import a_comma_separated_list, a_hdfs_file


DEFAULT_REDUCE_TASKS = max(3 * hadut.get_num_nodes(offline=True), 1)
DEFAULT_ENTRY_POINT = '__main__'
IS_JAVA_RR = "hadoop.pipes.java.recordreader"
IS_JAVA_RW = "hadoop.pipes.java.recordwriter"
CACHE_FILES = "mapred.cache.files"
CACHE_ARCHIVES = "mapred.cache.archives"
USER_HOME = "mapreduce.admin.user.home.dir"
JOB_REDUCES = "mapred.reduce.tasks"
JOB_NAME = "mapred.job.name"
COMPRESS_MAP_OUTPUT = "mapred.compress.map.output"
AVRO_IO_CHOICES = ['k', 'v', 'kv']
AVRO_IO_CHOICES += [_.upper() for _ in AVRO_IO_CHOICES]


class PydoopSubmitter(object):
    """
    Builds and launches pydoop jobs.
    """
    DESCRIPTION = "Simplified pydoop jobs submission"

    def __init__(self):
        hadoop_version_info = pydoop.hadoop_version_info()
        if hadoop_version_info.is_local():
            raise pydoop.LocalModeNotSupported()

        self.logger = logging.getLogger("PydoopSubmitter")
        self.properties = {
            CACHE_FILES: '',
            CACHE_ARCHIVES: '',
            'mapred.create.symlink': 'yes',  # backward compatibility
            COMPRESS_MAP_OUTPUT: 'true',
            'bl.libhdfs.opts': '-Xmx48m'
        }
        self.args = None
        self.requested_env = dict()
        self.remote_wd = None
        self.remote_module = None
        self.remote_module_bn = None
        self.remote_exe = None
        self.pipes_code = None
        self.files_to_upload = []
        self.unknown_args = None

    @staticmethod
    def __cache_archive_link(archive_name):
        # XXX: should we really be dropping the extension from the link name?
        return os.path.splitext(os.path.basename(archive_name))[0]

    def __set_files_to_cache_helper(self, prop, upload_and_cache, cache):
        cfiles = self.properties[prop] if self.properties[prop] else []
        cfiles += cache if cache else []
        if upload_and_cache:
            upf_to_cache = [
                ('file://' + os.path.realpath(e),
                 hdfs.path.join(self.remote_wd, bn),
                 bn if prop == CACHE_FILES else self.__cache_archive_link(e))
                for (e, bn) in ((e, os.path.basename(e))
                                for e in upload_and_cache)
            ]
            self.files_to_upload += upf_to_cache
            cached_files = ["%s#%s" % (h, b) for (_, h, b) in upf_to_cache]
            cfiles += cached_files
        self.properties[prop] = ','.join(cfiles)

    def __set_files_to_cache(self, args):
        if args.upload_file_to_cache is None:
            args.upload_file_to_cache = []
        self.__set_files_to_cache_helper(CACHE_FILES,
                                         args.upload_file_to_cache,
                                         args.cache_file)

    def __set_archives_to_cache(self, args):
        if args.upload_archive_to_cache is None:
            args.upload_archive_to_cache = []
        if args.python_zip:
            args.upload_archive_to_cache += args.python_zip
        self.__set_files_to_cache_helper(CACHE_ARCHIVES,
                                         args.upload_archive_to_cache,
                                         args.cache_archive)

    @staticmethod
    def _env_arg_to_dict(set_env_list):
        retval = dict()
        for item in set_env_list:
            try:
                name, value = item.split('=', 1)
                retval[name.strip()] = value.strip()
            except ValueError:
                raise RuntimeError(
                    "Bad syntax in env variable argument '%s'" % item
                )
        return retval

    def set_args(self, args, unknown_args=None):
        """
        Configure job, based on the arguments provided.
        """
        if unknown_args is None:
            unknown_args = []
        self.logger.setLevel(getattr(logging, args.log_level))

        parent = hdfs.path.dirname(hdfs.path.abspath(args.output.rstrip("/")))
        self.remote_wd = hdfs.path.join(
            parent, utils.make_random_str(prefix="pydoop_submit_")
        )
        self.remote_exe = hdfs.path.join(self.remote_wd, str(uuid.uuid4()))
        self.properties[JOB_NAME] = args.job_name or 'pydoop'
        self.properties[IS_JAVA_RR] = (
            'false' if args.do_not_use_java_record_reader else 'true'
        )
        self.properties[IS_JAVA_RW] = (
            'false' if args.do_not_use_java_record_writer else 'true'
        )
        self.properties[JOB_REDUCES] = args.num_reducers
        if args.job_name:
            self.properties[JOB_NAME] = args.job_name
        self.properties.update(dict(args.D or []))
        self.properties.update(dict(args.job_conf or []))
        self.__set_files_to_cache(args)
        self.__set_archives_to_cache(args)
        self.requested_env = self._env_arg_to_dict(args.set_env or [])
        self.args = args
        self.unknown_args = unknown_args

    def __warn_user_if_wd_maybe_unreadable(self, abs_remote_path):
        """
        Check directories above the remote module and issue a warning if
        they are not traversable by all users.

        The reasoning behind this is mainly aimed at set-ups with a
        centralized Hadoop cluster, accessed by all users, and where
        the Hadoop task tracker user is not a superuser; an example
        may be if you're running a shared Hadoop without HDFS (using
        only a POSIX shared file system).  The task tracker correctly
        changes user to the job requester's user for most operations,
        but not when initializing the distributed cache, so jobs who
        want to place files not accessible by the Hadoop user into
        dist cache fail.
        """
        host, port, path = hdfs.path.split(abs_remote_path)
        if host == '' and port == 0:  # local file system
            host_port = "file:///"
        else:
            # FIXME: this won't work with any scheme other than
            # hdfs:// (e.g., s3)
            host_port = "hdfs://%s:%s/" % (host, port)
        path_pieces = path.strip('/').split(os.path.sep)
        fs = hdfs.hdfs(host, port)
        for i in range(0, len(path_pieces)):
            part = os.path.join(
                host_port, os.path.sep.join(path_pieces[0: i + 1])
            )
            permissions = fs.get_path_info(part)['permissions']
            if permissions & 0o111 != 0o111:
                self.logger.warning(
                    ("remote module %s may not be readable by the task "
                     "tracker when initializing the distributed cache.  "
                     "Permissions on %s: %s"),
                    abs_remote_path, part, oct(permissions)
                )
                break

    def _generate_pipes_code(self):
        env = dict()
        for e in ('LD_LIBRARY_PATH', 'PATH', 'PYTHONPATH'):
            env[e] = ''
        lines = []
        if not self.args.no_override_env and not self.args.no_override_ld_path:
            env['LD_LIBRARY_PATH'] = os.environ.get('LD_LIBRARY_PATH', '')
        if not self.args.no_override_env and not self.args.no_override_path:
            env['PATH'] = os.environ.get('PATH', '')

        if not self.args.no_override_env and not self.args.no_override_pypath:
            env['PYTHONPATH'] = os.environ.get('PYTHONPATH', '')
        else:
            env['PYTHONPATH'] = "${PYTHONPATH}"

        # set user-requested env variables
        for var, value in self.requested_env.items():
            env[var] = value

        if self.args.pstats_dir:
            env[PSTATS_DIR] = self.args.pstats_dir
            if self.args.pstats_fmt:
                env[PSTATS_FMT] = self.args.pstats_fmt

        executable = self.args.python_program
        if self.args.python_zip:
            env['PYTHONPATH'] = ':'.join([
                self.__cache_archive_link(ar) for ar in self.args.python_zip
            ] + [env['PYTHONPATH']])
        # Note that we have to explicitely put the working directory
        # in the python path otherwise it will miss cached modules and
        # packages.
        env['PYTHONPATH'] = "${PWD}:" + env['PYTHONPATH']

        lines.append("#!/bin/bash")
        lines.append('""":"')
        if self.args.log_level == "DEBUG":
            lines.append("printenv 1>&2")
            lines.append("echo PWD=${PWD} 1>&2")
            lines.append("echo ls -l; ls -l  1>&2")
        if (
            USER_HOME not in self.properties and
            "HOME" in os.environ and
            not self.args.no_override_home
        ):
            lines.append('export HOME="%s"' % os.environ['HOME'])
        # set environment variables
        for var, value in env.items():
            if value:
                self.logger.debug("Setting env variable %s=%s", var, value)
                lines.append('export %s="%s"' % (var, value))
        if self.args.log_level == "DEBUG":
            lines.append("echo PATH=${PATH} 1>&2")
            lines.append("echo LD_LIBRARY_PATH=${LD_LIBRARY_PATH} 1>&2")
            lines.append("echo PYTHONPATH=${PYTHONPATH} 1>&2")
            lines.append("echo HOME=${HOME} 1>&2")
            lines.append('echo "executable is $(type -P %s)" 1>&2' %
                         executable)
        cmd = 'exec "%s" -u "$0" "$@"' % executable
        if self.args.log_level == 'DEBUG':
            lines.append("echo cmd to execute: %s" % cmd)
        lines.append(cmd)
        lines.append('":"""')
        if self.args.log_level == "DEBUG":
            lines.append('import sys')
            lines.append('sys.stderr.write("%r\\n" % sys.path)')
            lines.append('sys.stderr.write("%s\\n" % sys.version)')
        lines.append('import %s as module' % self.args.module)
        lines.append('module.%s()' % self.args.entry_point)
        return os.linesep.join(lines) + os.linesep

    def __validate(self):
        if not hdfs.path.exists(self.args.input):
            raise RuntimeError(
                "Input path %r does not exist" % (self.args.input,)
            )
        if hdfs.path.exists(self.args.output):
            raise RuntimeError(
                "Output path %r already exists" % (self.args.output,)
            )

    def __clean_wd(self):
        if self.remote_wd:
            try:
                self.logger.debug(
                    "Removing temporary working directory %s", self.remote_wd
                )
                hdfs.rmr(self.remote_wd)
            except IOError:
                pass

    def __setup_remote_paths(self):
        """
        Actually create the working directory and copy the module into it.

        Note: the script has to be readable by Hadoop; though this may not
        generally be a problem on HDFS, where the Hadoop user is usually
        the superuser, things may be different if our working directory is
        on a shared POSIX filesystem.  Therefore, we make the directory
        and the script accessible by all.
        """
        self.logger.debug("remote_wd: %s", self.remote_wd)
        self.logger.debug("remote_exe: %s", self.remote_exe)
        self.logger.debug("remotes: %s", self.files_to_upload)
        if self.args.module:
            self.logger.debug(
                'Generated pipes_code:\n\n %s', self._generate_pipes_code()
            )
        if not self.args.pretend:
            hdfs.mkdir(self.remote_wd)
            hdfs.chmod(self.remote_wd, "a+rx")
            self.logger.debug("created and chmod-ed: %s", self.remote_wd)
            pipes_code = self._generate_pipes_code()
            hdfs.dump(pipes_code, self.remote_exe)
            self.logger.debug("dumped pipes_code to: %s", self.remote_exe)
            hdfs.chmod(self.remote_exe, "a+rx")
            self.__warn_user_if_wd_maybe_unreadable(self.remote_wd)
            for (l, h, _) in self.files_to_upload:
                self.logger.debug("uploading: %s to %s", l, h)
                hdfs.cp(l, h)
        self.logger.debug("Created%sremote paths:" %
                          (' [simulation] ' if self.args.pretend else ' '))

    def run(self):
        if self.args is None:
            raise RuntimeError("cannot run without args, please call set_args")
        self.__validate()
        pydoop_classpath = []
        libjars = []
        if self.args.libjars:
            libjars.extend(self.args.libjars)
        if self.args.avro_input or self.args.avro_output:
            # append Pydoop's avro-mapred jar.  Don't put it at the front of
            # the list or the user won't be able to override it.
            avro_jars = glob.glob(os.path.join(
                pydoop.package_dir(), "avro*.jar"
            ))
            pydoop_classpath.extend(avro_jars)
            libjars.extend(avro_jars)
        pydoop_jar = pydoop.jar_path()
        if pydoop_jar is None:
            raise RuntimeError("Can't find pydoop.jar")
        job_args = []
        submitter_class = 'it.crs4.pydoop.mapreduce.pipes.Submitter'
        pydoop_classpath.append(pydoop_jar)
        libjars.append(pydoop_jar)
        self.logger.debug("Submitter class: %s", submitter_class)
        if self.args.hadoop_conf:
            job_args.extend(['-conf', self.args.hadoop_conf.name])
        if self.args.input_format:
            job_args.extend(['-inputformat', self.args.input_format])
        if self.args.output_format:
            job_args.extend(['-writer', self.args.output_format])
        job_args.extend(['-input', self.args.input])
        job_args.extend(['-output', self.args.output])
        job_args.extend(['-program', self.remote_exe])
        if libjars:
            job_args.extend(["-libjars", ','.join(libjars)])
        if self.args.avro_input:
            job_args.extend(['-avroInput', self.args.avro_input])
        if self.args.avro_output:
            job_args.extend(['-avroOutput', self.args.avro_output])
        if not self.args.disable_property_name_conversion:
            ctable = conv_tables.mrv1_to_mrv2
            props = [
                (ctable.get(k, k), v) for (k, v) in self.properties.items()
            ]
            self.properties = dict(props)
            self.logger.debug("properties after projection: %r",
                              self.properties)
        try:
            self.__setup_remote_paths()
            executor = (hadut.run_class if not self.args.pretend
                        else self.fake_run_class)
            executor(submitter_class, args=job_args,
                     properties=self.properties, classpath=pydoop_classpath,
                     logger=self.logger, keep_streams=False)
            self.logger.info("Done")
        finally:
            if not self.args.keep_wd:
                self.__clean_wd()

    def fake_run_class(self, *args, **kwargs):
        kwargs['logger'].info("Fake run class")
        repr_list = map(repr, args)
        repr_list.extend('%s=%r' % (k, v) for k, v in kwargs.items())
        sys.stdout.write("hadut.run_class(%s)\n" % ', '.join(repr_list))


def run(args, unknown_args=None):
    if unknown_args is None:
        unknown_args = []
    script = PydoopSubmitter()
    script.set_args(args, unknown_args)
    script.run()
    return 0


def add_parser_common_arguments(parser):
    parser.add_argument(
        '--num-reducers', metavar='INT', type=int,
        default=DEFAULT_REDUCE_TASKS,
        help="Number of reduce tasks. Specify 0 to only perform map phase"
    )
    parser.add_argument(
        '--no-override-home', action='store_true',
        help=("Don't set the script's HOME directory to the $HOME in your "
              "environment.  Hadoop will set it to the value of the "
              "'mapreduce.admin.user.home.dir' property")
    )
    parser.add_argument(
        '--no-override-env', action='store_true',
        help=("Use the default PATH, LD_LIBRARY_PATH and PYTHONPATH, instead "
              "of copying them from the submitting client node")
    )
    parser.add_argument(
        '--no-override-ld-path', action='store_true',
        help=("Use the default LD_LIBRARY_PATH instead of copying it from the "
              "submitting client node")
    )
    parser.add_argument(
        '--no-override-pypath', action='store_true',
        help=("Use the default PYTHONPATH instead of copying it from the "
              "submitting client node")
    )
    parser.add_argument(
        '--no-override-path', action='store_true',
        help=("Use the default PATH instead of copying it from the "
              "submitting client node")
    )
    parser.add_argument(
        '--set-env', metavar="VAR=VALUE", type=str, action="append",
        help=("Set environment variables for the tasks. If a variable "
              "is set to '', it will not be overridden by Pydoop.")
    )
    parser.add_argument(
        '-D', metavar="NAME=VALUE", type=kv_pair, action="append",
        help='Set a Hadoop property, e.g., -D mapred.compress.map.output=true'
    )
    parser.add_argument(
        '--python-zip', metavar='ZIP_FILE', type=str, action="append",
        help="Additional python zip file"
    )
    parser.add_argument(
        '--upload-file-to-cache', metavar='FILE', type=a_file_that_can_be_read,
        action="append",
        help="Upload and add this file to the distributed cache."
    )
    parser.add_argument(
        '--upload-archive-to-cache', metavar='FILE',
        type=a_file_that_can_be_read, action="append",
        help="Upload and add this archive file to the distributed cache."
    )
    parser.add_argument(
        '--log-level', metavar="LEVEL", default="INFO", help="Logging level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL", "FATAL"]
    )
    parser.add_argument(
        '--job-name', metavar='NAME', type=str, help="name of the job"
    )
    parser.add_argument(
        '--python-program', metavar='PYTHON', type=str, default=sys.executable,
        help="python executable that should be used by the wrapper"
    )
    parser.add_argument(
        '--pretend', action='store_true',
        help=("Do not actually submit a job, print the generated config "
              "settings and the command line that would be invoked")
    )
    parser.add_argument(
        '--hadoop-conf', metavar='HADOOP_CONF_FILE',
        type=a_file_that_can_be_read,
        help="Hadoop configuration file"
    )
    parser.add_argument(
        '--input-format', metavar='CLASS', type=str,
        help="java classname of InputFormat"
    )


def add_parser_arguments(parser):
    parser.add_argument(
        'module', metavar='MODULE', type=str,
        help=("The module containing the Python MapReduce program")
    )
    parser.add_argument(
        'input', metavar='INPUT', help='input path to the maps',
    )
    parser.add_argument(
        'output', metavar='OUTPUT', help='output path from the reduces',
    )
    parser.add_argument(
        '--disable-property-name-conversion', action='store_true',
        help="Do not adapt property names to the hadoop version used."
    )
    parser.add_argument(
        '--do-not-use-java-record-reader', action='store_true',
        help="Disable java RecordReader"
    )
    parser.add_argument(
        '--do-not-use-java-record-writer', action='store_true',
        help="Disable java RecordWriter"
    )
    parser.add_argument(
        '--output-format', metavar='CLASS', type=str,
        help="java classname of OutputFormat"
    )
    parser.add_argument(
        '--job-conf', metavar="NAME=VALUE", type=kv_pair, action="append",
        nargs='+',
        help='Set a Hadoop property, e.g., mapreduce.compress.map.output=true'
    )
    parser.add_argument(
        '--libjars', metavar='JAR_FILE', type=a_comma_separated_list,
        action="append", help="Additional comma-separated list of jar files"
    )
    parser.add_argument(
        '--cache-file', metavar='HDFS_FILE', type=a_hdfs_file,
        action="append",
        help="Add this HDFS file to the distributed cache as a file."
    )
    parser.add_argument(
        '--cache-archive', metavar='HDFS_FILE', type=a_hdfs_file,
        action="append",
        help="Add this HDFS archive file to the distributed cache" +
             "as an archive."
    )
    parser.add_argument(
        '--entry-point', metavar='ENTRY_POINT', type=str,
        default=DEFAULT_ENTRY_POINT,
        help=("Explicitly execute MODULE.ENTRY_POINT() "
              "in the launcher script.")
    )
    parser.add_argument(
        '--avro-input', metavar='k|v|kv', choices=AVRO_IO_CHOICES,
        help="Avro input mode (key, value or both)",
    )
    parser.add_argument(
        '--avro-output', metavar='k|v|kv', choices=AVRO_IO_CHOICES,
        help="Avro output mode (key, value or both)",
    )
    parser.add_argument(
        '--pstats-dir', metavar="HDFS_DIR", type=str,
        help="Profile each task and store stats in this dir"
    )
    parser.add_argument(
        '--pstats-fmt', metavar="STRING", type=str,
        help="pstats filename pattern (expert use only)"
    )
    parser.add_argument(
        '--keep-wd', action='store_true', help="Don't remove the work dir"
    )


def add_parser(subparsers):
    parser = subparsers.add_parser(
        "submit",
        description=PydoopSubmitter.DESCRIPTION,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    add_parser_common_arguments(parser)
    add_parser_arguments(parser)
    parser.set_defaults(func=run)
    return parser
