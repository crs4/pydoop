__author__ = 'kikkomep'

import jnius
from pydoop.pure.bridge.factory import ClassLoader


class PyjniusClassLoader(ClassLoader):

    def init(self, classpath, opts):
        pass

    def load_class(self, fully_qualified_class):
        return jnius.autoclass(fully_qualified_class)

    def close(self):
        pass