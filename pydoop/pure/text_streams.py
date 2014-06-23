from streams import DownStreamFilter, UpStreamFilter
from string_utils import quote_string, unquote_string

class TextDownStreamFilter(DownStreamFilter):
    """
    Naive textual stream filter implementation.

    It recognizes commands and their parameters expressed as a purely textual
    down_stream flow. 
    
    *NOTE:* this stream filter is intended for debugging purposes only.
    """
    SEP   = '\t'

    def __init__(self, stream):
        super(TextDownStreamFilter, self).__init__(stream)

    def next(self):
        line = self.stream.readline()[:-1]
        if len(line) == 0:
            raise StopIteration
        parts = line.split(self.SEP)
        return self.convert_message(parts[0], parts[1:])

class TextUpStreamFilter(UpStreamFilter):
    SEP   = '\t'
    EOL   = '\n'
    def __init__(self, stream):
        super(TextUpStreamFilter, self).__init__(stream)
    def send(self, cmd, *args):
        self.stream.write(cmd)
        for a in args:
            self.stream.write(self.SEP)
            self.stream.write(quote_string(str(a)))
        self.stream.write(self.EOL)
            
