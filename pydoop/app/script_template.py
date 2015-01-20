DRIVER_TEMPLATE = """
import sys, os, inspect
sys.path.insert(0, os.getcwd())

import pydoop.pipes
import %(module)s

class ContextWriter(object):

  def __init__(self, context):
    self.context = context
    self.counters = {}

  def emit(self, k, v):
    self.context.emit(str(k), str(v))

  def count(self, what, howmany):
    if self.counters.has_key(what):
      counter = self.counters[what]
    else:
      counter = self.context.getCounter('%(module)s', what)
      self.counters[what] = counter
    self.context.incrementCounter(counter, howmany)

  def status(self, msg):
    self.context.setStatus(msg)

  def progress(self):
    self.context.progress()

def setup_script_object(obj, fn_attr_name, user_fn, ctx):
  # Generic constructor for both map and reduce objects.
  #
  # Sets the 'writer' and 'conf' attributes.  Then, based on the arity
  # of the given user function (user_fn), sets the object attribute
  # (fn_attr_name, which should be either 'map' or 'reduce') to point
  # to either:
  #
  #   * obj.with_conf (when arity == 4)
  #   * obj.without_conf (when arity == 3)
  #
  # This way, when pipes calls the map/reduce function of the object
  # it actually gets either of the with_conf/without_conf functions
  # (which must be defined by the PydoopScriptMapper or
  # PydoopScriptReducer object passed into this function).
  #
  # Why all this?  The idea is to raise any decision about which
  # function to call out of the map/reduce functions, which get called
  # a number of times proportional to the amount of data to process.
  # On the other hand, the constructor only gets called once per task.
  if fn_attr_name not in ('map', 'reduce'):
    raise RuntimeError('Unexpected function attribute ' + fn_attr_name)
  obj.writer = ContextWriter(ctx)
  obj.conf = ctx.getJobConf()
  spec = inspect.getargspec(user_fn)
  if spec.varargs or len(spec.args) not in (3, 4):
    raise ValueError(user_fn +
       ' must take parameters key, value, writer, and optionally config)')
  if len(spec.args) == 3:
    setattr(obj, fn_attr_name, obj.without_conf)
  elif len(spec.args) == 4:
    setattr(obj, fn_attr_name, obj.with_conf)
  else:
    raise RuntimeError(
        'Unexpected number of %(map_fn)s arguments ' + len(spec.args))

class PydoopScriptMapper(pydoop.pipes.Mapper):
  def __init__(self, ctx):
    super(type(self), self).__init__(ctx)
    setup_script_object(self, 'map', %(module)s.%(map_fn)s, ctx)

  def without_conf(self, ctx):
    # old style map function, without the conf parameter
    writer = ContextWriter(ctx)
    %(module)s.%(map_fn)s(ctx.getInputKey(), ctx.getInputValue(), writer)

  def with_conf(self, ctx):
    # new style map function, without the conf parameter
    writer = ContextWriter(ctx)
    %(module)s.%(map_fn)s(ctx.getInputKey(), ctx.getInputValue(),
                          writer, self.conf)

  def map(self, ctx):
    pass

class PydoopScriptReducer(pydoop.pipes.Reducer):
  def __init__(self, ctx):
    super(type(self), self).__init__(ctx)
    setup_script_object(self, 'reduce', %(module)s.%(reduce_fn)s, ctx)

  @staticmethod
  def iter(ctx):
    while ctx.nextValue():
      yield ctx.getInputValue()

  def without_conf(self, ctx):
    key = ctx.getInputKey()
    writer = ContextWriter(ctx)
    %(module)s.%(reduce_fn)s(key, PydoopScriptReducer.iter(ctx), writer)

  def with_conf(self, ctx):
    key = ctx.getInputKey()
    writer = ContextWriter(ctx)
    %(module)s.%(reduce_fn)s(key, PydoopScriptReducer.iter(ctx),
                             writer, self.conf)

  def reduce(self, ctx):
    pass

class PydoopScriptCombiner(pydoop.pipes.Combiner):
  def __init__(self, ctx):
    super(type(self), self).__init__(ctx)
    setup_script_object(self, 'reduce', %(module)s.%(combine_fn)s, ctx)

  @staticmethod
  def iter(ctx):
    while ctx.nextValue():
      yield ctx.getInputValue()

  def without_conf(self, ctx):
    key = ctx.getInputKey()
    writer = ContextWriter(ctx)
    %(module)s.%(combine_fn)s(key, PydoopScriptCombiner.iter(ctx), writer)

  def with_conf(self, ctx):
    key = ctx.getInputKey()
    writer = ContextWriter(ctx)
    %(module)s.%(combine_fn)s(key, PydoopScriptReducer.iter(ctx),
                              writer, self.conf)

  def reduce(self, ctx):
    pass

def main():
    result = pydoop.pipes.runTask(
    pydoop.pipes.Factory(
        PydoopScriptMapper, PydoopScriptReducer,
        record_reader_class=None,
        record_writer_class=None, combiner_class=%(combiner_wp)s,
        partitioner_class=None))
"""
