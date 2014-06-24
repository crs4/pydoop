from streams import DownStreamFilter, UpStreamFilter
from serialize import deserialize, serialize

# these constants should be exactly what has been defined in HadoopPipes.cpp
START_MESSAGE = 0
SET_JOB_CONF  = 1
SET_INPUT_TYPES = 2
RUN_MAP = 3
MAP_ITEM = 4
RUN_REDUCE = 5
REDUCE_KEY = 6
REDUCE_VALUE = 7
CLOSE = 8
ABORT = 9
AUTHENTICATION_REQ = 10
OUTPUT = 50
PARTITIONED_OUTPUT = 51
STATUS = 52
PROGRESS = 53
DONE = 54
REGISTER_COUNTER = 55
INCREMENT_COUNTER = 56
AUTHENTICATION_RESP = 57

class BinaryWriter(object):
    CMD_CODE = {
        'start' : START_MESSAGE,
        'setJobConf' : SET_JOB_CONF,
        'setInputTypes' : SET_INPUT_TYPES,
        'runMap' : RUN_MAP,
        'mapItem' : MAP_ITEM,            
        'runReduce' : RUN_REDUCE,
        'reduceKey' : REDUCE_KEY,
        'reduceValue' : REDUCE_VALUE,        
        'close' : CLOSE,
        'abort' : ABORT,
        'authenticationReq' : AUTHENTICATION_REQ,
        'output' : OUTPUT,
        'partitionedOutput' : PARTITIONED_OUTPUT,
        'status' : STATUS,
        'progress' : PROGRESS,
        'done' : DONE,
        'registerCounter' : REGISTER_COUNTER,
        'incrementCounter' : INCREMENT_COUNTER,
        'authenticationResp' : AUTHENTICATION_RESP}        
        
    def __init__(self, stream):
        self.stream = stream
    def write(self, vals):
        serialize(self.CMD_CODE[vals[0]], self.stream)
        if vals[0] == 'setJobConf':
            serialize(len(vals[1:]), self.stream)
        for v in vals[1:]:
            serialize(v, self.stream)

class BinaryDownStreamFilter(DownStreamFilter):
    def get_list(stream):
        n = deserialize(int, stream)
        assert n >= 0
        return [deserialize(str, stream) for _ in range(n)]
    
    DFLOW_TABLE = {START_MESSAGE :   ('start', [int], None),
                   SET_JOB_CONF :    ('setJobConf', None, get_list),
                   SET_INPUT_TYPES : ('setInputTypes', [str, str], None),
                   RUN_MAP :         ('runMap', [str, int, int], None),
                   MAP_ITEM :        ('mapItem', [str, str], None),
                   RUN_REDUCE :      ('runReduce', [int, int], None),
                   REDUCE_KEY :      ('reduceKey', [str], None),
                   REDUCE_VALUE :    ('reduceValue', [str], None),
                   CLOSE :           ('close', [], None),
                   ABORT :           ('abort', [], None),
                   AUTHENTICATION_REQ : ('authenticationRequest',
                                         [str, str], None)
                   }
    def __init__(self, stream):
        super(BinaryDownStreamFilter, self).__init__(stream)
    def next(self):
        try:
            cmd_code = deserialize(int, self.stream)
        except EOFError:
            raise StopIteration
        cmd, types, processor = self.DFLOW_TABLE[cmd_code]
        if types is None: # no types, process stream directly
            args = processor(self.stream)
            return cmd, tuple(args)
        args = [deserialize(t, self.stream) for t in types]
        return cmd, tuple(args) if args else None        
        

class BinaryUpStreamFilter(UpStreamFilter):
    UPFLOW_TABLE =  {'output' :           (OUTPUT, [str, str], None),
                     'partitionedOutput' :(PARTITIONED_OUTPUT,   
                                             [int, str, str], None),
                     'status':            (STATUS, [str], None),
                     'progress' :         (PROGRESS, [float], None),
                     'done' :             (DONE, [], None),
                     'registerCounter' : (REGISTER_COUNTER,
                                           [int, str, str], None),
                     'incrementCounter' :  (INCREMENT_COUNTER,
                                             [int, int], None),
                     'authenticationResp' : (AUTHENTICATION_RESP,
                                              [str], None),
                     }
    
    def __init__(self, stream):
        super(BinaryUpStreamFilter, self).__init__(stream)        
    def send(self, cmd, *args):
        cmd_code, types, processor = self.UPFLOW_TABLE[cmd]
        serialize(cmd_code, stream)
        for t, v in zip(types, args):
            if t == float: # you never know...
                serialize(float(v), stream)
            else:
                assert t == type(v)
                serialize(v, stream)

        
