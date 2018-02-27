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
Pydoop Script
=============

A quick and easy to use interface for running simple MapReduce jobs.

Pydoop script is a front-end to pydoop submit that automatically builds
a map-reduce program using functions contained in a user provided
python module.

"""

import os
import warnings
import pydoop
import pydoop.hadut as hadut
import pydoop.utils as utils
import argparse
from tempfile import NamedTemporaryFile
from zipfile import ZipFile
from .submit import PydoopSubmitter, add_parser_common_arguments
from .script_template import DRIVER_TEMPLATE

DEFAULT_REDUCE_TASKS = max(3 * hadut.get_num_nodes(offline=True), 1)
DEFAULT_OUTPUT_FORMAT = 'org.apache.hadoop.mapred.TextOutputFormat'
NOSEP_OUTPUT_FORMAT = 'it.crs4.pydoop.NoSeparatorTextOutputFormat'

DESCRIPTION = "Simplified interface for running simple MapReduce jobs"


class PydoopScript(object):

    def __init__(self, args, unknown_args):
        self.script_archive = None
        self.args = None
        self.convert_args(args, unknown_args)

    @staticmethod
    def generate_driver(mr_module, args):
        combine_fn = args.combine_fn or args.reduce_fn
        combiner_wp = 'PydoopScriptCombiner' if args.combine_fn else 'None'
        return DRIVER_TEMPLATE.substitute(
            module=mr_module,
            map_fn=args.map_fn,
            reduce_fn=args.reduce_fn,
            combine_fn=combine_fn,
            combiner_wp=combiner_wp,
        )

    def convert_args(self, args, unknown_args):
        # Create a zip archive containing all we need to run the
        # script (including the script itself.  We use
        # NamedTemporaryFile which will take care of deleting the temp
        # archive once we're done
        self.script_archive = NamedTemporaryFile(
            prefix="pydoop_script_",
            suffix='.zip'
        )
        zip_filename = self.script_archive.name
        # Create a one-off temporary file name to avoid name clashes
        # in the distcache.  Keep the same module extension -- it may
        # be a source file or a byte-compiled file
        mr_module = utils.make_random_str(
            prefix="pydoop_script_module_",
            postfix=os.path.basename(args.module)
        )
        mr_driver = utils.make_random_str(prefix="pydoop_script_driver_")
        with ZipFile(zip_filename, 'w') as zipf:
            zipf.write(args.module, arcname=mr_module)
            zipf.writestr(
                mr_driver + '.py',
                self.generate_driver(os.path.splitext(mr_module)[0], args)
            )
        if args.python_zip is None:
            args.python_zip = [zip_filename]
        else:
            args.python_zip.append(zip_filename)
        args.module = mr_driver
        args.entry_point = 'main'
        args.program = mr_driver
        args.do_not_use_java_record_reader = False
        args.do_not_use_java_record_writer = False
        args.output_format = None
        args.cache_file = None
        args.cache_archive = None
        args.upload_to_cache = None
        args.libjars = None
        args.conf = None
        args.disable_property_name_conversion = True
        args.job_conf = [('mapred.textoutputformat.separator',
                          args.kv_separator)]
        args.avro_input = None
        args.avro_output = None
        args.keep_wd = False
        args.pstats_dir = None
        args.pstats_fmt = None

        # despicable hack...
        properties = dict(args.D or [])
        properties.update(dict(args.job_conf))
        output_format = properties.get('mapred.output.format.class',
                                       DEFAULT_OUTPUT_FORMAT)
        if output_format == DEFAULT_OUTPUT_FORMAT:
            if properties['mapred.textoutputformat.separator'] == '':
                pydoop_jar = pydoop.jar_path()
                if pydoop_jar is not None:
                    args.output_format = NOSEP_OUTPUT_FORMAT
                    args.libjars = [pydoop_jar]
                else:
                    warnings.warn(("Can't find pydoop.jar, output will "
                                   "probably be tab-separated"))
        self.args, self.unknown_args = args, unknown_args

    def run(self):
        submitter = PydoopSubmitter()
        submitter.set_args(self.args, self.unknown_args)
        submitter.run()
        return 0

    def clean(self):
        self.script_archive.close()


def run(args, unknown_args=None):
    if unknown_args is None:
        unknown_args = []
    scripter = PydoopScript(args, unknown_args)
    scripter.run()
    scripter.clean()
    return 0


def add_parser_arguments(parser):
    parser.add_argument('module', metavar='MODULE', help='python module file')
    parser.add_argument('input', metavar='INPUT', help='hdfs input path')
    parser.add_argument('output', metavar='OUTPUT', help='hdfs output path')
    parser.add_argument('-m', '--map-fn', metavar='MAP', default='mapper',
                        help="name of map function within module")
    parser.add_argument('-r', '--reduce-fn', metavar='RED', default='reducer',
                        help="name of reduce function within module")
    parser.add_argument('-c', '--combine-fn', metavar='COM', default=None,
                        help="name of combine function within module")
    parser.add_argument('--combiner-fn', metavar='COM', default=None,
                        help="--combine-fn alias for backwards compatibility")
    parser.add_argument('-t', '--kv-separator', metavar='SEP', default='\t',
                        help="output key-value separator")


def add_parser(subparsers):
    parser = subparsers.add_parser(
        "script",
        description=DESCRIPTION,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        epilog=("Hadoop pipes generic options are supported too.  "
                "Run `hadoop pipes` for more information")
    )
    add_parser_common_arguments(parser)
    add_parser_arguments(parser)
    parser.set_defaults(func=run)
    return parser
