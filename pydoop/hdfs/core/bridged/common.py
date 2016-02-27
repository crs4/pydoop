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

from pydoop.hdfs.core.bridged import get_wrapper_factory


FACTORY = get_wrapper_factory()


def wrap_class(fully_qualified_class_name):
    return FACTORY.get_wrapper(fully_qualified_class_name)


def wrap_class_instance(fully_qualified_class_name, *args):
    return FACTORY.get_wrapper(fully_qualified_class_name)(*args)


def wrap_array(fully_qualified_array_item_class, array_dimensions=1):
    return FACTORY.get_array_wrapper(
        fully_qualified_array_item_class, array_dimensions
    )


def wrap_array_instance(fully_qualified_array_item_class, array_dimension=1,
                        array_items=None):
    if array_items is None:
        array_items = []
    return FACTORY.get_array_wrapper_instance(
        fully_qualified_array_item_class, array_dimension, array_items
    )
