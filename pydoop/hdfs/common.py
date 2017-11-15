# BEGIN_COPYRIGHT
#
# Copyright 2009-2016 CRS4.
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
Common hdfs utilities.
"""

import getpass
import pwd
import grp
import sys
import os

__is_py3 = sys.version_info >= (3, 0)


BUFSIZE = 16384
DEFAULT_PORT = 8020  # org/apache/hadoop/hdfs/server/namenode/NameNode.java
DEFAULT_USER = getpass.getuser()
DEFAULT_LIBHDFS_OPTS = "-Xmx48m"  # enough for most applications

# Unicode objects are encoded using this encoding:
TEXT_ENCODING = 'utf-8'
# We use UTF-8 since this is what the Hadoop TextFileFormat uses
# NOTE:  If you change this, you'll also need to fix the encoding
# used by the native extension.


class Mode(object):

    VALUE = {
        "r": os.O_RDONLY,
        "w": os.O_WRONLY,
        "a": os.O_WRONLY | os.O_APPEND,
    }

    FLAGS = {
        os.O_RDONLY: "r",
        os.O_WRONLY: "w",
        os.O_WRONLY | os.O_APPEND: "a",
    }

    @property
    def value(self):
        return self.__value

    @property
    def flags(self):
        return self.__flags

    @property
    def binary(self):
        return self.__binary

    def __init__(self, m=None):
        self.__value = "r"
        self.__flags = os.O_RDONLY
        self.__binary = False
        if not m:
            return
        try:
            self.__value = m[0]
        except TypeError:
            try:
                self.__value = Mode.FLAGS[m]
            except KeyError:
                self.__error(m)
            else:
                self.__flags = m
        else:
            try:
                self.__flags = Mode.VALUE[self.__value]
            except KeyError:
                self.__error(m)
            else:
                try:
                    self.__binary = m[1] == "b"
                except IndexError:
                    pass

    def __error(self, m):
        raise ValueError("invalid mode: %r" % (m,))


if __is_py3:
    def encode_path(path):
        return path

    def decode_path(path):
        return path

    def encode_host(host):
        return host

    def decode_host(host):
        return host
else:
    def encode_path(path):
        if isinstance(path, unicode):  # noqa: F821
            path = path.encode('utf-8')
        return path

    def decode_path(path):
        if isinstance(path, str):
            path = path.decode('utf-8')
        return path

    def encode_host(host):
        if isinstance(host, unicode):  # noqa: F821
            host = host.encode('idna')
        return host

    def decode_host(host):
        if isinstance(host, str):
            host = host.decode('idna')
        return host


def get_groups(user=DEFAULT_USER):
    groups = set(_.gr_name for _ in grp.getgrall() if user in set(_.gr_mem))
    primary_gid = pwd.getpwnam(user).pw_gid
    groups.add(grp.getgrgid(primary_gid).gr_name)
    return groups
