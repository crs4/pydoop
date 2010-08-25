# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""
Contains just a trivial function that tells whether the given
character is a vowel.
"""

_VOWELS = set("AEIOUYaeiouy")

def is_vowel(c):
  """
  Find out if ``c`` is a vowel.

  :type c: string
  :param c: a character (length 1 string literal)
  :rtype: bool
  :return: True if ``c`` is a vowel, else False.
  """
  return c in _VOWELS
