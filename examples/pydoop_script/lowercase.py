# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""
Convert text to lowercase.

Set --kv-separator to the empty string when running this example.
"""

def mapper(_, record, writer):
  writer.emit("", record.lower())
