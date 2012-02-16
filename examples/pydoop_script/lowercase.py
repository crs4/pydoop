
# Convert text to lowercase.
# Input: text
# Output: lowercase text

# set --kv-separator ''
def mapper(ignored, record, writer):
	writer.emit("", record.lower())
