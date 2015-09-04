import sys
import os
import time
from kazoo.client import KazooClient

import logging
logging.basicConfig()

logger = logging.getLogger()
logger.setLevel(logging.INFO)

host = 'zookeeper'
port = int(os.environ['ZOO_CLIENT_PORT'])
logger.info('Starting on %s:%d', host, port)

kz = KazooClient(host, port)

path = '/' + sys.argv[1]
logger.info('Path is %s', path)

done = False
while not done:
    kz.start(timeout=15)
    done = kz.exists(path)
    kz.stop()
    time.sleep(10)
    


