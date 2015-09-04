import os
import sys
import ssl
import logging
from docker import tls
from docker import Client


logging.basicConfig()

logger = logging.getLogger('share_etc_hosts')
logger.setLevel(logging.DEBUG)


class App(object):
    def __init__(self, compose_group_name):
        self.client = docker_client()
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


def docker_client():
    """
    Returns a docker-py client configured using environment variables
    according to the same logic as the official Docker client.
    """
    cert_path = os.environ.get('DOCKER_CERT_PATH', '')
    if cert_path == '':
        cert_path = os.path.join(os.environ.get('HOME', ''), '.docker')

    base_url = os.environ.get('DOCKER_HOST')
    tls_config = None

    if os.environ.get('DOCKER_TLS_VERIFY', '') != '':
        parts = base_url.split('://', 1)
        base_url = '%s://%s' % ('https', parts[1])

        client_cert = (os.path.join(cert_path, 'cert.pem'), os.path.join(cert_path, 'key.pem'))
        ca_cert = os.path.join(cert_path, 'ca.pem')

        tls_config = tls.TLSConfig(
            ssl_version=ssl.PROTOCOL_TLSv1,
            verify=True,
            assert_hostname=False,
            client_cert=client_cert,
            ca_cert=ca_cert,
        )

    timeout = int(os.environ.get('DOCKER_CLIENT_TIMEOUT', 60))
    return Client(base_url=base_url, tls=tls_config, version='1.15', timeout=timeout)


def main(argv):
    tag = argv[1].replace('.', '').replace('_', '')
    logger.info('Tag is:%s', tag)
    app = App(tag)
    app.share_etc_hosts()


main(sys.argv)
