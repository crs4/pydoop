# BEGIN_COPYRIGHT
#
# Copyright 2009-2016 CRS4.
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
import importlib
from abc import abstractmethod, ABCMeta


class JavaWrapperFactory(object):

    _instance = None

    def __init__(self, java_bridge_name="JPype",
                 java_bridge_package="pydoop.utils.bridge",
                 classpath=None, opts=""):
        self._classpath = None
        if classpath is None and "CLASSPATH" in os.environ:
            self.set_classpath(os.environ.get("CLASSPATH", "."))
        else:
            self.set_classpath(classpath)
        self._instance = None
        self._bridge_name = java_bridge_name
        loader_class = load_class(java_bridge_package, java_bridge_name)
        self._loader = loader_class()
        self._loader.init(self._classpath, opts)

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(JavaWrapperFactory, cls).__new__(
                cls, *args, **kwargs)
        return cls._instance

    def close(self):
        self._loader.close()

    def set_classpath(self, classpath):
        self._classpath = classpath
        os.environ["CLASSPATH"] = self._classpath

    def get_classpath(self):
        return self._classpath

    def get_wrapper(self, fully_qualified_class):
        return self._loader.load_class(fully_qualified_class)

    def get_wrapper_instance(self, fully_qualified_class, *args):
        return self._loader.load_class(fully_qualified_class, *args)

    def get_array_wrapper(self, fully_qualified_array_item_class,
                          array_dimensions=1):
        return self._loader.load_array_class(
            fully_qualified_array_item_class, array_dimensions
        )

    def get_array_wrapper_instance(self, fully_qualified_array_item_class,
                                   array_dimensions=1, array_items=None):
        if array_items is None:
            array_items = []
        return self._loader.load_array_class(
            fully_qualified_array_item_class, array_dimensions
        )(array_items)


class ClassLoader(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def init(self, classpath, opts):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def load_class(self, fully_qualified_class):
        pass


def load_class_one(full_class_string):
    """
    dynamically load a class from a string
    """
    class_data = full_class_string.split(".")
    module_path = ".".join(class_data[:-1])
    class_str = class_data[-1]
    module = importlib.import_module(module_path)
    return getattr(module, class_str)


def load_class(package, bridge_name):
    """
    dynamically load a class from a string
    """
    module_name = bridge_name.lower() + "_loader"
    module_path = package + "." + module_name
    class_str = bridge_name + "ClassLoader"
    module = importlib.import_module(module_path)
    return getattr(module, class_str)
