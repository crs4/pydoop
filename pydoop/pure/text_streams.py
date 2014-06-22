from streams import DownStreamFilter, UpStreamFilter

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
