Testing pydoop using a Docker Cluster
=====================================

The purpose of the pydoop docker cluster is to provide a full, standard, hadoop
cluster that can be used for testing purposes. This is a "real" cluster, not a
pseudo-cluster single node thing.

The supported testing strategy is to do the following:

 #. choose and start an appropriate docker cluster;
 #. log in the 'client' node provided by the cluster;
 #. install on the client node the targeted hadoop version -- it should be
    compatible, at the protocol level should be enough, with the cluster;
 #. install on the client node the pydoop version under test;
 #. run pydoop tests and examples.


Docker cluster
--------------

Build a cluster
;;;;;;;;;;;;;;;

Clusters configurations are defined in subdirectories of the directory
``clusters``, e.g., ``clusters/apache_2.6.0``.

Do the following to build all the cluster independent images::

  $ cd clusters
  $ ../scripts/build_base_images.sh
  
Next, build all the cluster dependent images::

  $ ../scripts/build_cluster_images.sh apache_2.6.0

where we have used ``apache_2.6.0`` as an example.


Run a cluster
;;;;;;;;;;;;;

To start a cluster, do the following::

  $ ../scripts/start_cluster.sh apache_2.6.0
  No stopped containers
  Creating apache260_zookeeper_1...
  Creating apache260_bootstrap_1...
  Creating apache260_client_1...
  Creating apache260_namenode_1...
  Creating apache260_datanode_1...
  Creating apache260_historyserver_1...
  Creating apache260_resourcemanager_1...
  Creating apache260_nodemanager_1...

The script attemps to clean up left-overs from previous runs. Thus if it is not
the first time you have run it, it will ask for your permission to rm old containers::

  $ ../scripts/start_cluster.sh apache_2.6.0
  Stopping apache260_nodemanager_1...
  Stopping apache260_resourcemanager_1...
  Stopping apache260_historyserver_1...
  Stopping apache260_datanode_1...
  Stopping apache260_namenode_1...
  Stopping apache260_client_1...
  Stopping apache260_zookeeper_1...
  Going to remove apache260_nodemanager_1, apache260_resourcemanager_1, apache260_historyserver_1, apache260_client_1, apache260_datanode_1, apache260_namenode_1, apache260_bootstrap_1, apache260_zookeeper_1
  Are you sure? [yN] y
  Removing apache260_zookeeper_1...
  Removing apache260_bootstrap_1...
  Removing apache260_client_1...
  Removing apache260_namenode_1...
  Removing apache260_datanode_1...
  Removing apache260_historyserver_1...
  Removing apache260_resourcemanager_1...
  Removing apache260_nodemanager_1...
  Moved logs to logs.backup.12522
  Moved local to local.backup.12522
  Creating apache260_zookeeper_1...
  Creating apache260_bootstrap_1...
  Creating apache260_client_1...
  Creating apache260_namenode_1...
  Creating apache260_datanode_1...
  Creating apache260_historyserver_1...
  Creating apache260_resourcemanager_1...
  Creating apache260_nodemanager_1...


To check how the cluster is doing, look at the logs of the bootstrap node::

  $ cd apache_2.6.0
  $ docker-compose logs bootstrap
  Attaching to apache260_bootstrap_1
  bootstrap_1 | INFO:root:Starting bootstrap.
  bootstrap_1 | INFO:root:Waiting for /etc/hosts to update on bootstrap
  bootstrap_1 | INFO:root:Waiting for /etc/hosts to update on bootstrap
  bootstrap_1 | ....
  bootstrap_1 | INFO:root:Waiting for /etc/hosts to update on bootstrap
  bootstrap_1 | INFO:kazoo.client:Connecting to zookeeper:2181
  bootstrap_1 | INFO:kazoo.client:Zookeeper connection established, state: CONNECTED
  bootstrap_1 | INFO:root:Booting namenode
  bootstrap_1 | INFO:root:	done.
  bootstrap_1 | INFO:root:Booting datanode
  bootstrap_1 | INFO:root:	done.
  bootstrap_1 | Creating /mr-history/tmp
  bootstrap_1 | Creating /mr-history/done
  bootstrap_1 | Setting ownership (mapred:hadoop) and permissions for /mr-history
  bootstrap_1 | INFO:root:Booting resourcemanager
  bootstrap_1 | INFO:root:	done.
  bootstrap_1 | INFO:root:Booting nodemanager
  bootstrap_1 | INFO:root:	done.
  bootstrap_1 | INFO:root:Booting historyserver
  bootstrap_1 | INFO:root:	done.
  bootstrap_1 | INFO:root:Done with bootstrap.
  apache260_bootstrap_1 exited with code 0

Then check:

  #. the namenode, ``http://localhost:50070``, it should be up and reporting a
     datanode;
  #. the resourcemanager, ``http://localhost:8088``, it should be up and reporting a
     nodemanager;
  #. the historyserver, ``http://localhost:19888``.


How to use a docker cluster
---------------------------

These are the basic steps.

Change directory to ``client_side_tests``, choose a specific distribution, say
``apache_2.6.0`` and ``cd`` to that directory.

Run the following command::

  $ ../../scripts/start_client.sh [<PORT>]

The script will create a new docker container with a cluster client node that
will respond to ssh connections on port ``PORT``, with 3333 as its default
value.  The ``start_client.sh`` script will execute the bash script
``initialize.sh``, see the provided client side tests for examples, to install
on the client container the appropriate hadoop distribution, needed software,
and a set of utility scripts.

.. note::

  You will probably have to answer twice 'yes' to ssh paranoia.


Log in on the client, install pydoop and run the tests::

  $ ssh -p 3333 root@localhost
    Linux minas-morgul 3.18.7-gentoo #1 SMP Mon Feb 23 17:39:58 PST 2015 x86_64
    
    The programs included with the Debian GNU/Linux system are free software;
    the exact distribution terms for each program are described in the
    individual files in /usr/share/doc/*/copyright.

    Debian GNU/Linux comes with ABSOLUTELY NO WARRANTY, to the extent
    permitted by applicable law.
    root@client:~# su - aen -c "bash -x prepare_pydoop.sh"
    root@client:~# cd /home/aen/pydoop/
    root@client:~# python setup.py install
    root@client:~# cd
    root@client:~# su - aen -c "bash -x run_tests.sh"
    root@client:~# su - aen -c "bash -x run_examples.sh"    

Details
-------

Boostrap strategy
;;;;;;;;;;;;;;;;;

The main synchronization issues are:

 #. All hosts should be able to resolve logical names to IP, e.g., namenode
   wants to resolve datenodes' IP to their logical names

 #. Part of inter-services communication is handled by using shared hdfs
   directories that should be accessible with the appropriate permissions as a
   pre-condition to service firing up.


The boostrap strategy is as follows.

 #. There is an external mechanism -- here is the script
    ``../scripts/share_etc_hosts.py``, but it should really be integrated in
    docker-compose -- that guarantees that all nodes have in their ``/etc/hosts``
    entries for all nodes in the group.  We need to have an external mechanism
    that can talk to the docker server to be sure that we got all the nodes
    involved.

 #. We have a zookeper node that is guaranteed to be fired before any other
    service by having all other nodes linked to it in the docker-compose.yml
    file.

 #. We have an auxiliary service, boostrap, that is in charge of orchestrating
    the system boostrap.

 #. The expected boostrap workflow is as follows.

   a. docker-compose starts
   b. all services (except zookeper and bootstrap) wait until
      ``zookeeper:/<servicename>`` is set to ``boot``
   c. boostrap then does the following:
      
      1. waits until its /etc/hosts  has been changed;
      2. sets ``/{namenode,datanode}`` to boot;
      3. waits until namenode sets the ``/namenode`` to ``up``;
      4. creates the needed hdfs dirs with appropriate permissions;
      5. sets ``/{resourcemanager,nodemanager,historyserver}`` to ``boot``;
      6. dies gracefully.

