# BEGIN_COPYRIGHT
# END_COPYRIGHT

import pydoop.pipes as pp


class Reducer(pp.Reducer):

  def reduce(self, context):
    s = 0
    while context.nextValue():
      s += int(context.getInputValue())
    context.emit(context.getInputKey(), str(s))
