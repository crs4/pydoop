__author__ = 'kikkomep'

import jpype
from pydoop.utils.bridge.factory import ClassLoader


class JPypeClassLoader(ClassLoader):

    def init(self, classpath, opts):
        jvm = jpype.getDefaultJVMPath()
        if not jpype.isJVMStarted():
            jpype.startJVM(jvm, "-Djava.class.path="+classpath)

    def load_class(self, fully_qualified_class):
        return jpype.JClass(fully_qualified_class)

    def close(self):
        jpype.shutdownJVM()