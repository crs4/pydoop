#!/usr/bin/env python

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
Use simulators to run wordcount
===============================

This example shows how to use the two different version of the
HadoopSimulator to run wordcount with different configurations.
"""

import logging
import os
import shutil
import tempfile
import argparse
import sys
import stat

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(THIS_DIR, "data")
EXAMPLES_DIR = os.path.dirname(THIS_DIR)
WD = tempfile.mkdtemp(prefix="pydoop_")

USERS_CSV_FN = os.path.join(DATA_DIR, "users.csv")
AVRO_FN = os.path.join(DATA_DIR, "users.avro")
USERS_PETS_FN = os.path.join(DATA_DIR, "users_pets.avro")

WC_DIR = os.path.join(EXAMPLES_DIR, 'wordcount', 'bin')
if WC_DIR not in sys.path:
    sys.path.insert(0, WC_DIR)
AVRO_DIR = os.path.join(EXAMPLES_DIR, 'avro')
with open(os.path.join(AVRO_DIR, 'schemas', 'stats.avsc')) as f:
    STATS_SCHEMA_STR = f.read()
AVRO_PY_DIR = os.path.join(AVRO_DIR, 'py')
if AVRO_PY_DIR not in sys.path:
    sys.path.insert(0, AVRO_PY_DIR)

from pydoop.mapreduce.simulator import HadoopSimulatorNetwork
from pydoop.mapreduce.simulator import HadoopSimulatorLocal
from pydoop.mapreduce.pipes import InputSplit
from pydoop.avrolib import AvroContext
from pydoop.utils.py3compat import iteritems
import pydoop.mapreduce.pipes as pp
import pydoop.test_support as pts

from wordcount_minimal import FACTORY as factory_minimal
from wordcount_full import FACTORY as factory_full
import avro_base
import check_results as avro_check_results
import avro_container_dump_results

LOG_LEVELS = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL", "FATAL"]
WC_TYPES = 'full', 'minimal'
SIMS = 'local', 'network'
AVRO_MODES = 'k', 'v', 'kv'
DATA_OUT = '/tmp/pydoop-test-avro_out'


AVRO_MAPPERS = {
    'k': avro_base.AvroKeyColorPick,
    'v': avro_base.AvroValueColorPick,
    'kv': avro_base.AvroKeyValueColorPick,
}

AVRO_REDUCERS = {
    'k': avro_base.AvroKeyColorCount,
    'v': avro_base.AvroValueColorCount,
    'kv': avro_base.AvroKeyValueColorCount,
    None: avro_base.NoAvroColorCount,
}

AVRO_APPS = {
    ('k', None): 'avro_key_in.py',
    ('v', None): 'avro_value_in.py',
    ('kv', None): 'avro_key_value_in.py',
    ('k', 'k'): 'avro_key_in_out.py',
    ('v', 'v'): 'avro_value_in_out.py',
    ('kv', 'kv'): 'avro_key_value_in_out.py',
}


def cp_script(script):
    dest = os.path.join(WD, os.path.basename(script))
    with open(script) as f, open(dest, "w") as fo:
        fo.write(pts.set_python_cmd(f.read()))
    os.chmod(dest, 0o755)
    return dest


def create_configuration():
    data_in = os.path.join(EXAMPLES_DIR, 'input', 'alice_1.txt')
    data_out = 'results.txt'
    data_in_uri = 'file://%s' % data_in
    data_in_size = os.stat(data_in).st_size
    output_dir = tempfile.mkdtemp(prefix="pydoop_")
    output_dir_uri = 'file://%s' % output_dir
    conf = {
        "mapred.map.tasks": "2",
        "mapred.reduce.tasks": "1",
        "mapred.job.name": "wordcount",
        "mapred.work.output.dir": output_dir_uri,
        "mapred.task.partition": "0",
    }
    input_split = InputSplit.to_string(data_in_uri, 0, data_in_size)
    return data_in, data_out, conf, input_split, output_dir


def dump_counters(hs, logger):
    counters = hs.get_counters()
    for phase in ['mapping', 'reducing']:
        logger.info("%s counters:", phase.capitalize())
        for group in counters[phase]:
            logger.info("  Group %s", group)
            for c, v in iteritems(counters[phase][group]):
                logger.info("   %s: %s", c, v)


def clean_up(data_out, output_dir):
    os.unlink(data_out)
    shutil.rmtree(output_dir)


def check_results(data_in, data_out, logger):
    local_wc = pts.LocalWordCount(data_in)
    logger.info("checking results")
    with open(data_out, 'r') as f:
        res = local_wc.check(f.read())
    if res.startswith("ERROR"):  # FIXME: change local_wc to raise an exception
        logger.error(res)
        raise RuntimeError(res)
    logger.info(res)


def run_network_minimal(logger):
    program_name = cp_script(os.path.join(WC_DIR, 'wordcount_minimal.py'))
    data_in, data_out, conf, input_split, output_dir = create_configuration()
    hs = HadoopSimulatorNetwork(program=program_name, logger=logger,
                                loglevel=logger.level)
    hs.run(open(data_in), open(data_out, 'wb'), conf)
    dump_counters(hs, logger)
    check_results(data_in, data_out, logger)
    clean_up(data_out, output_dir)


def run_local_minimal(logger):
    hs = HadoopSimulatorLocal(factory=factory_minimal, logger=logger,
                              loglevel=logger.level)
    data_in, data_out, conf, input_split, output_dir = create_configuration()
    hs.run(open(data_in), open(data_out, 'wb'), conf)
    dump_counters(hs, logger)
    check_results(data_in, data_out, logger)
    clean_up(data_out, output_dir)


def run_local_full(logger):
    data_in, data_out, conf, input_split, output_dir = create_configuration()
    hsl = HadoopSimulatorLocal(factory=factory_full, logger=logger,
                               loglevel=logger.level)
    hsl.run(None, None, conf, input_split=input_split, num_reducers=1)
    data_out = os.path.join(output_dir,
                            'part-r-%05d' % int(conf["mapred.task.partition"]))
    dump_counters(hsl, logger)
    check_results(data_in, data_out, logger)
    clean_up(data_out, output_dir)


def run_network_full(logger):
    program_name = cp_script(os.path.join(WC_DIR, 'wordcount_full.py'))
    data_in, data_out, conf, input_split, output_dir = create_configuration()
    hs = HadoopSimulatorNetwork(program=program_name, logger=logger,
                                loglevel=logger.level)
    hs.run(None, None, conf, input_split=input_split)
    data_out = os.path.join(output_dir,
                            'part-r-%05d' % int(conf["mapred.task.partition"]))
    dump_counters(hs, logger)
    check_results(data_in, data_out, logger)
    clean_up(data_out, output_dir)


def run_local_avro(logger, avro_in='v', avro_out=None):
    mapper, reducer = AVRO_MAPPERS[avro_in], AVRO_REDUCERS[avro_out]
    schema_k_out = STATS_SCHEMA_STR if avro_out in {'k', 'kv'} else None
    schema_v_out = STATS_SCHEMA_STR if avro_out in {'v', 'kv'} else None
    file_in = USERS_PETS_FN if avro_in == 'kv' else AVRO_FN
    factory = pp.Factory(mapper_class=mapper, reducer_class=reducer)
    simulator = HadoopSimulatorLocal(
        factory, logger, logging.INFO, AvroContext,
        avro_in, avro_out, schema_k_out, schema_v_out
    )
    with open(file_in, 'rb') as fin, open(DATA_OUT, 'wb') as fout:
        simulator.run(fin, fout, {}, num_reducers=1)
    dump_counters(simulator, logger)
    if avro_out:
        data_out_des = DATA_OUT + '-des'
        avro_container_dump_results.main(DATA_OUT, data_out_des, avro_out)
        avro_check_results.main(USERS_CSV_FN, data_out_des)
    else:
        avro_check_results.main(USERS_CSV_FN, DATA_OUT)


def run_network_avro(logger, avro_in='v', avro_out=None):
    try:
        program_name = AVRO_APPS[(avro_in, avro_out)]
    except KeyError:
        raise ValueError(
            "not supported: avro_in=%s, avro_out=%s" % (avro_in, avro_out)
        )
    else:
        program = os.path.join(WD, program_name)
        for name in program_name, "avro_base.py":
            shutil.copy(os.path.join(AVRO_PY_DIR, name), WD)
        os.chmod(program, os.stat(program).st_mode | stat.S_IEXEC)
    file_in = USERS_PETS_FN if avro_in == 'kv' else AVRO_FN
    schema_k_out = STATS_SCHEMA_STR if avro_out in {'k', 'kv'} else None
    schema_v_out = STATS_SCHEMA_STR if avro_out in {'v', 'kv'} else None
    simulator = HadoopSimulatorNetwork(
        program, logger, logging.INFO, context_cls=AvroContext,
        avro_input=avro_in, avro_output=avro_out,
        avro_output_key_schema=schema_k_out,
        avro_output_value_schema=schema_v_out
    )
    with open(file_in, 'rb') as fin, open(DATA_OUT, 'wb') as fout:
        simulator.run(fin, fout, {}, num_reducers=1)
    dump_counters(simulator, logger)
    if avro_out:
        data_out_des = DATA_OUT + '-des'
        avro_container_dump_results.main(DATA_OUT, data_out_des, avro_out)
        avro_check_results.main(USERS_CSV_FN, data_out_des)
    else:
        avro_check_results.main(USERS_CSV_FN, DATA_OUT)


def make_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--log-level', metavar="LEVEL", default="INFO", choices=LOG_LEVELS,
        help="one of: %s" % "; ".join(LOG_LEVELS)
    )
    parser.add_argument('--wc', metavar='|'.join(WC_TYPES), choices=WC_TYPES)
    parser.add_argument('--sim', metavar='|'.join(SIMS), choices=SIMS)
    parser.add_argument('--avro-in', metavar='|'.join(AVRO_MODES),
                        choices=AVRO_MODES)
    parser.add_argument('--avro-out', metavar='|'.join(AVRO_MODES),
                        choices=AVRO_MODES)
    return parser


def main(argv):
    parser = make_parser()
    args = parser.parse_args(argv)
    if args.avro_out and not args.avro_in:
        parser.error("avro_in must be set if avro_out is")
    if args.wc and args.avro_in:
        parser.error("specify either wc or an avro mode")
    logger = logging.getLogger("main")
    logger.setLevel(getattr(logging, args.log_level))
    if args.avro_in:
        logger.info("running avro input=%s, output=%s with %s simulator" %
                    (args.avro_in, args.avro_out, args.sim))
        runf = globals()["run_%s_avro" % args.sim]
        runf(logger, avro_in=args.avro_in, avro_out=args.avro_out)
    else:
        logger.info("running %s wc with %s simulator", args.wc, args.sim)
        runf = globals()["run_%s_%s" % (args.sim, args.wc)]
        runf(logger)


if __name__ == "__main__":
    main(sys.argv[1:])
