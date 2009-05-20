#!/usr/bin/env python

import sys
import subprocess
import glob
import random

def get_hosts_for_job_id(job_id):
  fs = glob.glob('/drbd/GE/6.1u5/default/spool/*/active_jobs/%s.1' % job_id)
  hosts = []
  for f in fs:
    f = f.replace('/drbd/GE/6.1u5/default/spool/', '')
    f = f.replace('/active_jobs/%s.1' % job_id)
    hosts.append(f)
  return hosts

def is_hadoop_running_on_host(h):
  o = subprocess.Popen(['ssh', h, 'ps', 'aux'],
                       stdout=subprocess.PIPE).communicate()[0]
  return o.find('hadoop.hdfs') > -1


def create_seqs(sdir, hosts):
  #os.mkdir(sdir)
  n = len(hosts)
  k = 0
  while 2**k < n:
    i = 2**k
    hs = random.sample(hosts, i)
    print hs
    k += 1
  print random.sample(hosts, n)

def main():
  job_id   = sys.argv[1]
  seqs_dir = sys.argv[2]
  hosts = get_hosts_for_job_id(job_id)
  hosts_with_hdp_running = []
  for h in hosts:
    if is_hadoop_running_on_host(h):
      hosts_with_hdp_running.append(h)
  create_seqs(seqs_dir, hosts_with_hdp_running)




