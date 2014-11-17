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
