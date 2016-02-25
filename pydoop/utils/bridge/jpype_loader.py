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

import jpype
from .factory import ClassLoader


class JPypeClassLoader(ClassLoader):

    wrapper_for_java_builtin_type = {
        "byte": jpype.JByte,
        # "short": jpype.JShort,
        "int": jpype.JInt,
        "long": jpype.JLong,
        "float": jpype.JFloat,
        "double": jpype.JBoolean,
        "char": jpype.JChar,
        "String": jpype.JString
    }

    def init(self, classpath, opts):
        jvm = jpype.getDefaultJVMPath()
        if not jpype.isJVMStarted():
            jpype.startJVM(jvm, "-Djava.class.path=" + classpath)

    def load_class(self, fully_qualified_class):
        return jpype.JClass(self.process_class_name(fully_qualified_class))

    def load_array_class(self, fully_qualified_array_item_class,
                         array_dimension=1):
        return jpype.JArray(
            self.process_class_name(fully_qualified_array_item_class),
            array_dimension
        )

    def process_class_name(self, fully_qualified_class_name):
        wrapper = self.wrapper_for_java_builtin_type.get(
            fully_qualified_class_name
        )
        if wrapper is not None:
            fully_qualified_class_name = wrapper
        return fully_qualified_class_name

    def close(self):
        jpype.shutdownJVM()
