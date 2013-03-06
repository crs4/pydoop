.. _mr_api:

:mod:`pydoop.pipes` --- MapReduce API
=====================================

.. automodule:: pydoop.pipes
   :members:


Classes Instantiated by the Framework
-------------------------------------

The following classes are not accessible to the MapReduce
programmer. The framework instantiates context objects and passes them
as parameters to some of the methods (e.g., ``map``, ``reduce``\ ) you
define. Through the context, you get access to several objects and
methods related to the job's current configuration and state.


.. class:: TaskContext

   Provides information about the task and the job. This is the base
   class for :class:`MapContext` and :class:`ReduceContext`\ .

   .. method:: getJobConf()

   Get the current job configuration.

   :rtype: :class:`JobConf`
   :return: the job configuration object

   Get the JobConf for the current task.

   .. method:: getInputKey()
   
   Get the current input key.

   :rtype: string
   :return: the current input key

   .. method:: getInputValue()
   
   Get the current input value.

   :rtype: string
   :return: the current input value

   .. method:: emit(key, value)
   
   Generate an output record.

   :param key: the intermediate key
   :type key: string
   :param value: the intermediate value
   :type value: string

   .. method:: progress()

   Mark your task as having made progress without changing the status message.

   .. method:: setStatus(status)

   Set the status message and call progress.

   :param status: the status message
   :type status: string

   .. method:: getCounter(group, name)
   
   Register a :class:`Counter` with the given group and name.
   
   :param group: a counter group
   :type group: string
   :param name: a counter name
   :type name: string

   .. method:: incrementCounter(counter, amount)

   Increment the value of the counter with the given amount.

   :param counter: an application counter
   :type counter: :class:`Counter`
   :param amount: the increment value
   :type amount: int


.. class:: MapContext

   Provides information about the map task and the job. Inherited from
   :class:`TaskContext`\ .

   .. method:: getInputSplit()

   Access the (serialized) ``InputSplit`` of the :class:`Mapper`\ .

   This is a raw byte string that should not be used directly, but
   rather passed to :class:`InputSplit`'s constructor.

   :rtype: string
   
   .. method:: getInputKeyClass()

   Get the name of the key class of the input to this task.

   :rtype: string

   .. method:: getInputValueClass()

   Get the name of the value class of the input to this task.

   :rtype: string


.. class:: ReduceContext

   Provides information about the reduce task and the job. Inherited from
   :class:`TaskContext`\ .

   .. method::  nextValue()

   Advance to the next value.

   :rtype: bool


.. class:: JobConf

   A JobConf defines the properties for a job.

   .. method:: hasKey(key)

   Return :obj:`True` if ``key`` is a configuration parameter.

   :param key: the name of a configuration parameter
   :type key: string
   :rtype: bool
   :return: :obj:`True` if ``key`` is present in this JobConf object.

   .. method:: get(key)
      
   Get the value of the configuration parameter ``key``\ .

   :param key: the name of a configuration parameter
   :type key: string
   :rtype: string
   :return: the value of the configuration parameter ``key``

   .. method:: getInt(key)
      
   Get the value of the configuration parameter ``key`` as an integer.

   :param key: the name of a configuration parameter
   :type key: string
   :rtype: int
   :return: the value of the configuration parameter ``key`` as an integer.

   .. method:: getFloat(key)
      
   Get the value of the configuration parameter ``key`` as a float.

   :param key: the name of a configuration parameter
   :type key: string
   :rtype: float
   :return: the value of the configuration parameter ``key`` as a float.

   .. method:: getBoolean(key)
      
   Get the value of the configuration parameter ``key`` as a boolean.

   :param key: the name of a configuration parameter
   :type key: string
   :rtype: bool
   :return: the value of the configuration parameter ``key`` as a boolean.


..  class:: Counter(id)

    Keeps track of a property and its value.

    .. method:: getId()

    :rtype: int
    :return: counter id
