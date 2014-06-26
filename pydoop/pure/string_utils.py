QUOTE_MAP = {
    '\\': '\\\\',
    '\t': '\\t',
    '\n': '\\n',
    ' ': '\\s',
    }

UNQUOTE_MAP = {
    't': '\t',
    'n': '\n',
    's': ' ',
    'X': '\\',
    }


def quote_string(in_string, deliminators='\\'):
    return ''.join(s if (32 < ord(s) < 127) and s not in deliminators
                     else QUOTE_MAP.get(s, r'\%02x' % ord(s))
                   for s in in_string)

def unquote_head(p):
    return (UNQUOTE_MAP[p[0]] + p[1:]) \
           if p[0] not in '0123456789' \
           else chr(int(p[0:2], 16)) + p[2:]


def unquote_string(in_string):
    #FIXME HACK HACK
    s = in_string.replace('\\\\', '\\X')
    parts = s.split('\\')
    return ''.join([parts[0]] + [unquote_head(p) for p in parts[1:]])
