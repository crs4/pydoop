#!/usr/bin/env python

# BEGIN_COPYRIGHT
#
# Copyright 2009-2017 CRS4.
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

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
EXAMPLES_DIR = os.path.dirname(THIS_DIR)
WD = tempfile.mkdtemp(prefix="pydoop_")

WC_DIR = os.path.join(EXAMPLES_DIR, 'wordcount', 'bin')
if WC_DIR not in sys.path:
    sys.path.insert(0, WC_DIR)
AVRO_DIR = os.path.join(EXAMPLES_DIR, 'avro')
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
import create_input
import write_avro
import avro_container_dump_results

LOG_LEVELS = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL", "FATAL"]
DATA_OUT = '/tmp/pydoop-test-avro_out'
STATS_SCHEMA = os.path.join(AVRO_DIR, 'schemas', 'stats.avsc')


def cp_script(script):
    dest = os.path.join(WD, os.path.basename(script))
    with open(script) as f, open(dest, "w") as fo:
        fo.write(pts.set_python_cmd(f.read()))
    os.chmod(dest, 0o755)
    return dest


def create_configuration():
    data_in = os.path.join(EXAMPLES_DIR, 'input', 'alice.txt')
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
    hs.run(open(data_in), open(data_out, 'w'), conf)
    dump_counters(hs, logger)
    check_results(data_in, data_out, logger)
    clean_up(data_out, output_dir)


def run_local_full(logger):
    data_in, data_out, conf, input_split, output_dir = create_configuration()
    hsl = HadoopSimulatorLocal(factory=factory_full, logger=logger,
                               loglevel=logger.level)
    hsl.run(None, None, conf, input_split=input_split, num_reducers=1)
    data_out = os.path.join(output_dir,
                            'part-%05d' % int(conf["mapred.task.partition"]))
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
                            'part-%05d' % int(conf["mapred.task.partition"]))
    dump_counters(hs, logger)
    check_results(data_in, data_out, logger)
    clean_up(data_out, output_dir)


def prepare_avro_data(csv_fn, avro_fn, schema_fn):
    create_input.main(20, csv_fn)
    write_avro.main(schema_fn, csv_fn, avro_fn)


def run_local_avro(
        mapper,
        reducer,
        logger,
        file_in,
        data_in,
        avro_input=None,
        avro_output=None,
        schema_k_out=None,
        schema_v_out=None
):
    factory = pp.Factory(mapper_class=mapper, reducer_class=reducer)
    hsl = HadoopSimulatorLocal(
        factory,
        logger,
        logging.INFO,
        AvroContext,
        avro_input,
        avro_output,
        schema_k_out,
        schema_v_out
    )
    hsl.run(open(file_in), open(DATA_OUT, 'w'), {}, num_reducers=1)
    dump_counters(hsl, logger)
    if avro_output:
        data_out_des = DATA_OUT + '-des'
        avro_container_dump_results.main(DATA_OUT, data_out_des, avro_output)
        avro_check_results.main(data_in, data_out_des)
    else:
        avro_check_results.main(data_in, DATA_OUT)


def run_network_avro(
        program_name,
        logger,
        file_in,
        data_in,
        avro_input=None,
        avro_output=None,
        schema_k_out=None,
        schema_v_out=None
):
    hsl = HadoopSimulatorNetwork(
        program_name,
        logger,
        logging.INFO,
        context_cls=AvroContext,
        avro_input=avro_input,
        avro_output=avro_output,
        avro_output_key_schema=schema_k_out,
        avro_output_value_schema=schema_v_out
    )
    hsl.run(open(file_in), open(DATA_OUT, 'w'), {}, num_reducers=1)
    dump_counters(hsl, logger)
    if avro_output:
        data_out_des = DATA_OUT + '-des'
        avro_container_dump_results.main(DATA_OUT, data_out_des, avro_output)
        avro_check_results.main(data_in, data_out_des)
    else:
        avro_check_results.main(data_in, DATA_OUT)


def make_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--log-level', metavar="LEVEL", default="INFO", choices=LOG_LEVELS,
        help="one of: %s" % "; ".join(LOG_LEVELS)
    )
    return parser


def main(argv):
    parser = make_parser()
    args = parser.parse_args(argv)
    logger = logging.getLogger("main")
    logger.setLevel(getattr(logging, args.log_level))
    logger.info("*** word count full using HadoopSimulatorNetwork ***")
    run_network_full(logger)
    logger.info("*** word count minimal using HadoopSimulatorNetwork ***")
    run_network_minimal(logger)
    return
    logger.info("*** word count minimal using HadoopSimulatorLocal ***")
    run_local_minimal(logger)
    logger.info("*** word count full using HadoopSimulatorLocal ***")
    run_local_full(logger)

    csv_fn = os.path.realpath(os.path.abspath('data/users.csv'))
    avro_fn = os.path.realpath(os.path.abspath('data/users.avro'))

    with open(STATS_SCHEMA) as schema_f:
        stats_schema_str = schema_f.read()

    users_pets_fn = os.path.realpath(os.path.abspath('data/users_pets.avro'))

    logger.info("*** avro_input v using HadoopSimulatorLocal ***")
    run_local_avro(avro_base.AvroValueColorPick, avro_base.NoAvroColorCount,
                   logger, avro_fn, csv_fn, avro_input='v')

    logger.info("*** avro_input/ouput v using HadoopSimulatorLocal ***")
    run_local_avro(
        avro_base.AvroValueColorPick, avro_base.AvroValueColorCount,
        logger, avro_fn, csv_fn, 'v', 'v', None, stats_schema_str
    )

    logger.info("*** avro_input k using HadoopSimulatorLocal ***")
    run_local_avro(avro_base.AvroKeyColorPick, avro_base.NoAvroColorCount,
                   logger, avro_fn, csv_fn, 'k')

    logger.info("*** avro_input/output k using HadoopSimulatorLocal ***")
    run_local_avro(
        avro_base.AvroKeyColorPick, avro_base.AvroKeyColorCount,
        logger, avro_fn, csv_fn, 'k', 'k', stats_schema_str
    )

    logger.info("*** avro_input kv using HadoopSimulatorLocal ***")
    run_local_avro(avro_base.AvroKeyValueColorPick, avro_base.NoAvroColorCount,
                   logger, users_pets_fn, csv_fn, 'kv')

    logger.info("*** avro_input/output kv using HadoopSimulatorLocal ***")
    run_local_avro(
        avro_base.AvroKeyValueColorPick, avro_base.AvroKeyValueColorCount,
        logger, users_pets_fn, csv_fn, 'kv', 'kv', stats_schema_str,
        stats_schema_str
    )

    logger.info("*** avro_input v using HadoopSimulatorNetwork ***")
    run_network_avro(os.path.join(AVRO_PY_DIR, 'avro_value_in.py'), logger,
                     avro_fn, csv_fn, 'v', None)

    logger.info("*** avro_input k using HadoopSimulatorNetwork ***")
    run_network_avro(os.path.join(AVRO_PY_DIR, 'avro_key_in.py'), logger,
                     avro_fn, csv_fn, 'k', None)

    logger.info("*** avro_input kv using HadoopSimulatorNetwork ***")
    run_network_avro(os.path.join(AVRO_PY_DIR, 'avro_key_value_in.py'), logger,
                     users_pets_fn, csv_fn, 'kv', None)

    logger.info("*** avro_input/output v using HadoopSimulatorNetwork ***")
    run_network_avro(os.path.join(AVRO_PY_DIR, 'avro_value_in_out.py'), logger,
                     avro_fn, csv_fn, 'v', 'v', None, stats_schema_str)

    logger.info("*** avro_input/output k using HadoopSimulatorNetwork ***")
    run_network_avro(os.path.join(AVRO_PY_DIR, 'avro_key_in_out.py'), logger,
                     avro_fn, csv_fn, 'k', 'k', stats_schema_str)

    logger.info("*** avro_input/output kv using HadoopSimulatorNetwork ***")
    run_network_avro(
        os.path.join(AVRO_PY_DIR, 'avro_key_value_in_out.py'), logger,
        users_pets_fn, csv_fn, 'kv', 'kv', stats_schema_str, stats_schema_str
    )


if __name__ == "__main__":
    main(sys.argv[1:])
