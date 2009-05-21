#!/usr/bin/env python

import sys
import subprocess
import glob
import random
import os

def get_job_id(d):
  fs = glob.glob(os.path.join(d, '*.state'))
  assert len(fs) == 1
  f = fs[0]
  return f[f.rfind('/'):f.rfind('.')]

def get_hosts_for_job_id(job_id):
  pat = '/drbd/GE/6.1u5/default/spool/*/active_jobs/%s.1' % job_id
  fs = glob.glob(pat)
  hosts = []
  for f in fs:
    f = f.replace('/drbd/GE/6.1u5/default/spool/', '')
    f = f[0: f.find('/active_jobs/')]
    hosts.append(f)
  return hosts

def is_hadoop_running_on_host(h):
  o = subprocess.Popen(['ssh', h, 'ps', 'aux'],
                       stdout=subprocess.PIPE).communicate()[0]
  return o.find('hadoop.hdfs') > -1


def write_seq(path, hosts):
  f = open(path , "w")
  for h in hosts:
    f.write("%s\n" % h)
  f.close()
  
def create_seqs(sdir, hosts):
  os.mkdir(sdir)
  n = len(hosts)
  k = 0
  i = 2**k  
  while i < n:
    write_seq(os.path.join(sdir, "%d.list" % i), random.sample(hosts, i))
    k += 1
    i = 2**k
  write_seq(os.path.join(sdir, "%d.list" % i), random.sample(hosts, n))    

def main():
  job_id   = get_job_id(sys.argv[1])
  seqs_dir = sys.argv[2]
  hosts = get_hosts_for_job_id(job_id)
  hosts_with_hdp_running = []
  for h in hosts:
    if is_hadoop_running_on_host(h):
      hosts_with_hdp_running.append(h)
  create_seqs(seqs_dir, hosts_with_hdp_running)

main()


