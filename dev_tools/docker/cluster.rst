Apache 2.6.0 Docker Cluster
===========================






How to use this docker cluster
------------------------------

bash$ ./scripts/build_base_images.sh 
bash$ cd clusters
bash$ ../scripts/build_cluster_images.sh apache_260
bash$ ../scripts/start_cluster.sh apache_260
bash$ 


 scp -P 2222 run_client.sh root@localhost:
scp -P 2222 run_client.sh root@localhost:
ssh -p 2222 root@localhost

zag@pflip apache_2.6.0 (docker)]$ ssh -p 2222 root@localhost
root@localhost's password: 
Permission denied, please try again.
root@localhost's password: 
Linux minas-morgul 3.18.7-gentoo #1 SMP Mon Feb 23 17:39:58 PST 2015 x86_64

The programs included with the Debian GNU/Linux system are free software;
the exact distribution terms for each program are described in the
individual files in /usr/share/doc/*/copyright.

Debian GNU/Linux comes with ABSOLUTELY NO WARRANTY, to the extent
permitted by applicable law.
root@client:~# bash ./run_client.sh 


explain why we share /usr/local and how we use the client.



Boostrap strategy
-----------------

The main synchronization issues are:

1. All hosts should be able to resolve logical names to IP, e.g., namenode
   wants to resolve datenodes' IP to their logical names

2. Part of inter-services communication is handled by using shared hdfs
   directories that should be accessible with the appropriate permissions
   as a pre-condition to service firing up.


The boostrap strategy is as follows.

1. There is an external mechanism -- here is the script
   '../scripts/share_etc_hosts.py', but it should really be integrated in
   docker-compose -- that guarantees that all nodes have in their /etc/hosts
   entries for all nodes in the group.  We need to have an external mechanism
   that can talk to the docker server to be sure that we got all the nodes
   involved.

1. We have a zookeper node that is guaranteed to be fired before any other
   service by having all other nodes linked to it in the docker-compose.yml
   file.

1. We have an auxiliary service, boostrap, that is in charge of orchestrating
   the system boostrap.

1. The expected boostrap workflow is as follows.

   a. docker-compose starts
   b. all services (except zookeper and bootstrap) wait until
      zookeeper:/<servicename> is set to 'boot'
   c. boostrap then does the following:
      1. waits until its /etc/hosts  has been changed;
      2. sets /{namenode,datanode} to boot;
      3. waits until namenode sets the /namenode to 'up';
      4. creates the needed hdfs dirs with appropriate permissions;
      5. sets /{resourcemanager,nodemanager,historyserver} to 'boot';
      6. dies gracefully.

