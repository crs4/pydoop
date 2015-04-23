import os
import sys
import platform
from docker.tls import TLSConfig
from docker import Client
import logging
logging.basicConfig()

logger = logging.getLogger('share_etc_hosts')
logger.setLevel(logging.DEBUG)


class App(object):
    def __init__(self, compose_group_name):
        if platform.system() == 'Darwin':
            docker_port = "2376"
            docker_host = os.environ['DOCKER_HOST_IP']
            docker_cert_path = os.environ['DOCKER_CERT_PATH']
            docker_base_url = "https://" + docker_host + ":" + docker_port
            docker_cert = os.path.join(docker_cert_path, 'cert.pem')
            docker_key = os.path.join(docker_cert_path, 'key.pem')
            tls_config = TLSConfig(client_cert=(docker_cert, docker_key),
                                   verify=False)
            self.client = Client(base_url=docker_base_url, tls=tls_config)
        else:
            self.client = Client(base_url='unix://var/run/docker.sock')
        self.containers = self._get_containers(compose_group_name)

    def _get_containers(self, compose_group_name):
        head = '/%s_' % compose_group_name
        cs = [c for c in self.client.containers()
              if c['Names'][0].startswith(head)]
        return cs

    def _get_hosts(self):
        hosts = {}
        for c in self.containers:
            d = self.client.inspect_container(c['Id'])
            hosts[c['Id']] = (d['NetworkSettings']['IPAddress'],
                              d['Config']['Hostname'])
        return hosts

    def share_etc_hosts(self):
        hosts = self._get_hosts()
        host_table = str('\n'.join(['%s\t%s' % h for h in hosts.itervalues()]))
        logger.debug('Host table is:\n%s', host_table)
        cmd = '/bin/bash -c "echo -e %r >> /etc/hosts"' % host_table
        for k in hosts:
            logger.debug('Updating %s', k)
            print self.client.execute(k, cmd)


def main(argv):
    tag = argv[1].replace('.', '').replace('_', '')
    logger.info('Tag is:%s', tag)
    app = App(tag)
    app.share_etc_hosts()


main(sys.argv)
