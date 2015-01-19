# BEGIN_COPYRIGHT
#
# Copyright 2009-2015 CRS4.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# END_COPYRIGHT

import os


MAPREDUCE_TASK_IO_SORT_MB_KEY = "mapreduce.task.io.sort.mb"
MAPREDUCE_TASK_IO_SORT_MB = 100
ENVIRONMENT_CMD_KEYS = {
    '0.20.2': {
        'file': 'hadoop.pipes.command.port',
        'port': 'hadoop.pipes.command.port',
        'secret_location': 'hadoop.pipes.shared.secret.location',
    },
    '0.20.2-cdh3u4': {
        'file': 'hadoop.pipes.command.port',
        'port': 'hadoop.pipes.command.port',
        'secret_location': 'hadoop.pipes.shared.secret.location',
    },
    '0.20.2-cdh3u5': {
        'file': 'hadoop.pipes.command.port',
        'port': 'hadoop.pipes.command.port',
        'secret_location': 'hadoop.pipes.shared.secret.location',
    },
    '1.0.4': {
        'file': 'hadoop.pipes.command.port',
        'port': 'hadoop.pipes.command.port',
        'secret_location': 'hadoop.pipes.shared.secret.location',
    },
    '1.0.4.patched': {
        'file': 'hadoop.pipes.command.port',
        'port': 'hadoop.pipes.command.port',
        'secret_location': 'hadoop.pipes.shared.secret.location',
    },
    '1.1.2': {
        'file': 'hadoop.pipes.command.port',
        'port': 'hadoop.pipes.command.port',
        'secret_location': 'hadoop.pipes.shared.secret.location',
    },
    '1.2.1': {
        'file': 'hadoop.pipes.command.port',
        'port': 'hadoop.pipes.command.port',
        'secret_location': 'hadoop.pipes.shared.secret.location',
    },
    '2.0.0-cdh4.2.0': {
        'file': 'hadoop.pipes.commandport',
        'port': 'mapreduce.pipes.command.port',
        'secret_location': 'hadoop.pipes.shared.secret.location',
    },
    '2.0.0-cdh4.3.0': {
        'file': 'hadoop.pipes.commandport',
        'port': 'mapreduce.pipes.command.port',
        'secret_location': 'hadoop.pipes.shared.secret.location',
    },
    '2.0.0-mr1-cdh4.2.0': {
        'file': 'hadoop.pipes.commandport',
        'port': 'mapreduce.pipes.command.port',
        'secret_location': 'hadoop.pipes.shared.secret.location',
    },
    '2.0.0-mr1-cdh4.3.0': {
        'file': 'hadoop.pipes.commandport',
        'port': 'mapreduce.pipes.command.port',
        'secret_location': 'hadoop.pipes.shared.secret.location',
    },
    '2.2.0': {
        'file': 'hadoop.pipes.commandport',
        'port': 'mapreduce.pipes.command.port',
        'secret_location': 'hadoop.pipes.shared.secret.location',
    },
}
_PORT_KEYS = set(v["port"] for v in ENVIRONMENT_CMD_KEYS.itervalues())
_FILE_KEYS = set(v["file"] for v in ENVIRONMENT_CMD_KEYS.itervalues())
_SECRET_LOCATION_KEYS = set(
    v["secret_location"] for v in ENVIRONMENT_CMD_KEYS.itervalues()
)
_ENV_KEYS = set(os.environ)


def resolve_environment_port_key():
    try:
        return (_ENV_KEYS & _PORT_KEYS).pop()
    except KeyError:
        return None


def resolve_environment_port(key=None):
    if key is None:
        key = resolve_environment_port_key()
    return None if key is None else os.getenv(key)


def resolve_environment_file_key():
    try:
        return (_ENV_KEYS & _FILE_KEYS).pop()
    except KeyError:
        return None


def resolve_environment_file(key=None):
    if key is None:
        key = resolve_environment_file_key()
    return None if key is None else os.getenv(key)


def resolve_environment_secret_location_key():
    try:
        return (_ENV_KEYS & _SECRET_LOCATION_KEYS).pop()
    except KeyError:
        return "hadoop.pipes.shared.secret.location"


def resolve_environment_secret_location(key=None):
    if key is None:
        key = resolve_environment_secret_location_key()
    return None if key is None else os.getenv(key)
