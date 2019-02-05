# BEGIN_COPYRIGHT
#
# Copyright 2009-2019 CRS4.
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
import shutil
import string
import subprocess
import sys
import tempfile
import fnmatch


JPROG = string.Template("""\
public class ${classname} {
  public static void main(String[] args) {
    System.out.println(System.getProperty("java.home"));
  }
}
""")


def get_java_home():
    """\
    Try getting JAVA_HOME from system properties.

    We are interested in the JDK home, containing include/jni.h, while the
    java.home property points to the JRE home. If a JDK is installed, however,
    the two are (usually) related: the JDK home is either the same directory
    as the JRE home (recent java versions) or its parent (and java.home points
    to jdk_home/jre).
    """
    error = RuntimeError("java home not found, try setting JAVA_HOME")
    try:
        return os.environ["JAVA_HOME"]
    except KeyError:
        wd = tempfile.mkdtemp(prefix='pydoop_')
        jclass = "Temp"
        jsrc = os.path.join(wd, "%s.java" % jclass)
        with open(jsrc, "w") as f:
            f.write(JPROG.substitute(classname=jclass))
        try:
            subprocess.check_call(["javac", jsrc])
            path = subprocess.check_output(
                ["java", "-cp", wd, jclass], universal_newlines=True
            )
        except (OSError, UnicodeDecodeError, subprocess.CalledProcessError):
            raise error
        finally:
            shutil.rmtree(wd)
        path = os.path.normpath(path.strip())
        if os.path.exists(os.path.join(path, "include", "jni.h")):
            return path
        path = os.path.dirname(path)
        if os.path.exists(os.path.join(path, "include", "jni.h")):
            return path
        raise error


def load_jvm_lib(java_home=None):
    if not java_home:
        java_home = get_java_home()
    jvm_path, jvm_lib = get_jvm_lib_path_and_name(java_home)
    if jvm_path and jvm_lib:
        from ctypes import CDLL
        CDLL(os.path.join(jvm_path, jvm_lib))
    else:
        raise ImportError("Unable to load the JVM dynamic library")


def get_include_dirs():
    java_home = get_java_home()
    dirs = [os.path.join(java_home, 'include'),
            os.path.join('native', 'jni_include'),
            os.path.join(java_home, 'lib')]
    if sys.platform == 'win32':
        dirs += [os.path.join(java_home, 'include', 'win32')]
    elif sys.platform == 'darwin':
        dirs += [os.path.join(java_home, 'include', 'darwin')]
    elif sys.platform.startswith('freebsd'):
        dirs += [os.path.join(java_home, 'include', 'freebsd')]
    else:  # linux
        dirs += [os.path.join(java_home, 'include', 'linux')]
    return dirs


def get_libraries():
    libraries = []
    if sys.platform == 'win32':
        libraries += ['Advapi32']
    elif sys.platform == 'darwin':
        libraries += ['dl', 'jvm']
    elif sys.platform.startswith('freebsd'):
        libraries += ['jvm']
    else:  # linux etc.
        libraries += ['dl', "jvm"]
    return libraries


def get_macros():
    macros = []
    if sys.platform == 'win32':
        macros += [('WIN32', 1)]
    elif sys.platform == 'darwin':
        macros += [('MACOSX', 1)]
    else:  # linux etc.
        pass
    return macros


def get_jvm_lib_path_and_name(java_home=None):
    if not java_home:
        java_home = get_java_home()
    jvm_lib_name = None
    if sys.platform == 'win32':
        jvm_lib_name = "jvm.dll"  # FIXME: check the library name
    elif sys.platform == 'darwin':
        jvm_lib_name = "libjvm.dylib"
    else:  # linux
        jvm_lib_name = "libjvm.so"
    jvm_path = find_file(java_home, jvm_lib_name)
    return os.path.dirname(jvm_path), jvm_lib_name if jvm_path else None


def check_jni_header(include_dirs=None):
    for d in include_dirs:
        if os.path.exists(os.path.join(d, 'jni.h')):
            found_jni = True
            break
    if not found_jni:
        import warnings
        warnings.warn('Falling back to provided JNI headers: ' +
                      'unable to find jni.h in your JAVA_HOME')


def find_file(path, to_find):
    result = None
    for element in os.listdir(path):
        if result:
            break
        if fnmatch.fnmatch(element, to_find):
            fullPath = os.path.join(path, element)
            result = fullPath
        if not result and os.path.isdir(os.path.join(path, element)):
            result = find_file(os.path.join(path, element), to_find)
    return result
