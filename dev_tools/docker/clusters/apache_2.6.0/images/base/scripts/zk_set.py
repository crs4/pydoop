import sys
import os
import time
from kazoo.client import KazooClient

import logging
logging.basicConfig()

logger = logging.getLogger()
logger.setLevel(logging.INFO)


kz = KazooClient('zookeeper', int(os.environ['ZOO_CLIENT_PORT']))

path  = '/' + sys.argv[1]
value = sys.argv[2]

kz.start()
kz.set(path, value)
kz.stop()    


