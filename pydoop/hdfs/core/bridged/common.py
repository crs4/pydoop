from pydoop.hdfs.core.bridged import get_factory_wrapper_factory

factory = get_factory_wrapper_factory()


def wrap_class(fully_qualified_class_name):
    """

    :param fully_qualified_class_name:
    :return:
    """
    return factory.get_wrapper(fully_qualified_class_name)


def wrap_class_instance(fully_qualified_class_name, *args):
    """

    :param fully_qualified_class_name:
    :param args:
    :return:
    """
    return factory.get_wrapper(fully_qualified_class_name)(*args)


def wrap_array(fully_qualified_array_item_class, array_dimensions=1):
    """

    :param fully_qualified_array_item_class:
    :param array_dimensions:
    :return:
    """
    return factory.get_array_wrapper(fully_qualified_array_item_class, array_dimensions)


def wrap_array_instance(fully_qualified_array_item_class, array_dimension=1, array_items=[]):
    """

    :param fully_qualified_array_item_class:
    :param array_dimension:
    :param array_items:
    :return:
    """
    return factory.get_array_wrapper_instance(fully_qualified_array_item_class, array_dimension, array_items)
