from abc import abstractmethod, ABCMeta

__author__ = 'kikkomep'

import os
import importlib


class JavaWrapperFactory(object):
    _instance = None

    def __init__(self,
                 java_bridge_name="JPype",
                 java_bridge_package="pydoop.utils.bridge",
                 classpath=None, opts=""):

        self._classpath = None
        if classpath is None and os.environ.has_key("CLASSPATH"):
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
    # Finally, we retrieve the Class
    return getattr(module, class_str)


def load_class(package, bridge_name):
    """
    dynamically load a class from a string
    """
    module_name = bridge_name.lower() + "_loader"
    module_path = package + "." + module_name
    class_str = bridge_name+"ClassLoader"

    module = importlib.import_module(module_path)
    # Finally, we retrieve the Class
    return getattr(module, class_str)


if __name__ == '__main__':
    print "JAVABRIDGE: " + os.environ.get("JAVA_BRIDGE", "NONE")

    os.environ["JAVA_BRIDGE"] = "jpype"

    loader = JavaWrapperFactory(java_bridge_name="pyjnius")

    # print loader.get_classpath()

    Stack = loader.get_wrapper('java.util.Stack')
    stack = Stack()

    stack.push('hello')
    stack.push('world')

    print stack.pop()
    print stack.pop()

    Configuration = loader.get_wrapper("org.apache.hadoop.conf.Configuration")
    conf = Configuration(True)

    loader.close()