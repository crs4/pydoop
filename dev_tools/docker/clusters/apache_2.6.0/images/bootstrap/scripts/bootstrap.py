from kazoo.client import KazooClient
import os
import time
import logging
import platform

logging.basicConfig()

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# FIXME this will break if our name is a substring of the hosts we are linked
# to.
def etc_updated():
    hostname = platform.node()
    logger.info('Waiting for /etc/hosts to update on %s', hostname)
    if not hostname:
        raise RuntimeError('hostname is undefined')
    with open('/etc/hosts') as f:
        return sum(x.find(hostname) > -1 for x in f) > 1
    logger.info('\tdone')

def boot_node(kz, nodename):
    logger.info('Booting %s', nodename)
    path = '/' + nodename
    kz.create(path, 'boot')
    while kz.get(path)[0] != 'up':
        time.sleep(2)
    logger.info('\tdone.')


def main():
    logger.info('Starting bootstrap.')
    zookeeper_host = 'zookeeper'
    zookeeper_port = int(os.environ['ZOO_CLIENT_PORT'])
    while not etc_updated():
        time.sleep(1)
    kz = KazooClient(hosts='%s:%d' % (zookeeper_host, zookeeper_port))
    kz.start()
    boot_node(kz, 'namenode')
    boot_node(kz, 'datanode')
    os.system('bash /tmp/create_hdfs_dirs.sh')
    boot_node(kz, 'resourcemanager')
    boot_node(kz, 'nodemanager')
    boot_node(kz, 'historyserver')    
    logger.info('Done with bootstrap.')

main()
    
