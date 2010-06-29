# BEGIN_COPYRIGHT
# END_COPYRIGHT

import pydoop.pipes as pp
from cv.lib import is_vowel


import os  # DEBUG

class Mapper(pp.Mapper):

  os.system("ls -R .")

  def map(self, context):
    for c in context.getInputValue():
      if is_vowel(c):
        context.emit(c.upper(), "1")
