Introduction
============

Pydoop includes several usage examples: you can find them in the
"examples" subdirectory of the distribution root. To run them, you
need a working Hadoop cluster. If you don't have one available, you
can bring up a single-node Hadoop cluster on your machine following
the `Hadoop quickstart guide
<http://hadoop.apache.org/common/docs/r0.20.2/quickstart.html>`_\
. Configure Hadoop for "Pseudo-Distributed Operation" and start the
daemons as explained in the guide.

Pydoop applications are run as any other Hadoop Pipes applications
(e.g., C++ ones)::

  ${HADOOP_HOME}/bin/hadoop pipes -conf conf.xml -input input -output output

Where ``input`` and ``output`` are, respectively, the HDFS directory
where the applications will read its input and write its output. The
configuration file, read from the local file system, is an xml
document consisting of a simple name = value property list:

.. code-block:: xml

  <?xml version="1.0"?>
  <configuration>
  
  <property>
    <name>hadoop.pipes.executable</name>
    <value>app_executable</value>
  </property>
  
  <property>
    <name>mapred.job.name</name>
    <value>app_name</value>
  </property>
  
  <property>
    <name>hadoop.pipes.java.recordreader</name>
    <value>true</value>
  </property>
  
  <property>
    <name>hadoop.pipes.java.recordwriter</name>
    <value>true</value>
  </property>
  
  [...]

  </configuration>

``hadoop.pipes.executable`` is the HDFS path (either absolute or
relative to your HDFS home directory) of the application launcher
(i.e., the one that contains the :func:`~pydoop.pipes.runTask`
invocation); ``mapred.job.name`` is just an identifier for your
application: it will appear in the MapReduce web interface and it will
be appended to the job log file name. The remaining two properties
must be set to ``false`` if you are using your own customized
RecordReader / RecordWriter.

In the job configuration file you can set either general Hadoop
properties (e.g., ``mapred.map.tasks``\ ) or application-specific
properties: the latter are configuration parameters defined by the
application developer, whose value will be retrieved at run time
through the :class:`~pydoop.pipes.JobConf` object.

To summarize, before running your application, you need to perform the
following steps:

 * upload the application launcher and the input file or directory to HDFS
 * prepare the xml configuration file, indicating the HDFS path to the
   launcher as shown above; alternatively, you can specify the
   launcher's path it by passing it as an argument to the ``-program``
   command line option
 * check that the ``output`` directory does not exists (it will not be
   overwritten: an error will be generated instead)


Modifying your launcher to auto-set environment
-----------------------------------------------

When working on a shared cluster where you don't have root access, you
might have a lot of software installed in non-standard locations, such
as your home directory. Since non-interactive ssh connections do not
read your ``~/.profile`` or ``~/.bashrc``\ , you might lose some
essential setting like ``LD_LIBRARY_PATH``\ .

A quick way to fix this is to do something like the following:

.. code-block:: bash

  #!/bin/sh
  
  """:"
  export LD_LIBRARY_PATH="my/lib/path:${LD_LIBRARY_PATH}"
  exec /path/to/pyexe/python -u $0 $@
  ":"""
  
  # Python code for the launcher follows

In this way, the launcher is run as a shell script that does some
exports and then executes Python on itself. Note that sh code is
protected by a Python comment, so that it's not considered when the
script is interpreted by Python. Here is a quick Python script that
performs the above launcher adjustment (it assumes you've already set
the desired ``LD_LIBRARY_PATH`` in your ``.profile`` or ``.bashrc``\
):

.. code-block:: python

  import sys, os
  
  NEW_HEADER = """#!/bin/sh
  
  ""\":"
  export LD_LIBRARY_PATH="%s"
  exec %s -u $0 $@
  ":""\"
  """
    
  try:
    script = sys.argv[1]
  except IndexError:
    sys.exit("Usage: %s SCRIPT" % sys.argv[0])
  try:
    LD_LIBRARY_PATH = os.environ["LD_LIBRARY_PATH"]
  except KeyError:
    sys.exit("ERROR: could not get LD_LIBRARY_PATH!")
  f = open(script)
  code = f.read()
  f.close()
  of = open("%s.bak" % script, "w")
  of.write(code)
  of.close()
  if code.startswith("#!"):
    code = code.split(os.linesep, 1)[1]
  code = NEW_HEADER % (LD_LIBRARY_PATH, sys.executable) + code
  of = open(script, "w")
  of.write(code)
  of.close()


Hadoop 0.21.0 notes
-------------------

The 0.21.0 release of Hadoop introduced `a consistent amount of
changes
<http://hadoop.apache.org/common/docs/r0.21.0/releasenotes.html>`_. Luckily,
in most cases, the old way of doing things is still supported,
although it triggers deprecation warnings (e.g., when setting a
property using its old name). A behavior that will almost surely break
your application is trying to *read* a property using its old name,
because most of the Java-side code has been updated to use the new
ones.

Throughout the documentation we will use pre-0.21.0 property
names. The following table maps pre-0.21.0 property names to their
0.21.0 counterparts:

============================================  ======
PRE-0.21.0                                    0.21.0
============================================  ======
create.empty.dir.if.nonexist                  mapreduce.jobcontrol.createdir.ifnotexist
hadoop.job.history.location                   mapreduce.jobtracker.jobhistory.location
hadoop.job.history.user.location              mapreduce.job.userhistorylocation
hadoop.net.static.resolutions                 mapreduce.tasktracker.net.static.resolutions
hadoop.pipes.command-file.keep                mapreduce.pipes.commandfile.preserve
hadoop.pipes.executable                       mapreduce.pipes.executable
hadoop.pipes.executable.interpretor           mapreduce.pipes.executable.interpretor
hadoop.pipes.java.mapper                      mapreduce.pipes.isjavamapper
hadoop.pipes.java.recordreader                mapreduce.pipes.isjavarecordreader
hadoop.pipes.java.recordwriter                mapreduce.pipes.isjavarecordwriter
hadoop.pipes.java.reducer                     mapreduce.pipes.isjavareducer
hadoop.pipes.partitioner                      mapreduce.pipes.partitioner
io.sort.factor                                mapreduce.task.io.sort.factor
io.sort.mb                                    mapreduce.task.io.sort.mb
io.sort.spill.percent                         mapreduce.map.sort.spill.percent
job.end.notification.url                      mapreduce.job.end-notification.url
job.end.retry.attempts                        mapreduce.job.end-notification.retry.attempts
job.end.retry.interval                        mapreduce.job.end-notification.retry.interval
job.local.dir                                 mapreduce.job.local.dir
jobclient.completion.poll.interval            mapreduce.client.completion.pollinterval
jobclient.output.filter                       mapreduce.client.output.filter
jobclient.progress.monitor.poll.interval      mapreduce.client.progressmonitor.pollinterval
keep.failed.task.files                        mapreduce.task.files.preserve.failedtasks
keep.task.files.pattern                       mapreduce.task.files.preserve.filepattern
key.value.separator.in.input.line             mapreduce.input.keyvaluelinerecordreader.key.value.separator
local.cache.size                              mapreduce.tasktracker.cache.local.size
map.input.file                                mapreduce.map.input.file
map.input.length                              mapreduce.map.input.length
map.input.start                               mapreduce.map.input.start
map.output.key.field.separator                mapreduce.map.output.key.field.separator
map.output.key.value.fields.spec              mapreduce.fieldsel.map.output.key.value.fields.spec
mapred.binary.partitioner.left.offset         mapreduce.partition.binarypartitioner.left.offset
mapred.binary.partitioner.right.offset        mapreduce.partition.binarypartitioner.right.offset
mapred.cache.archives                         mapreduce.job.cache.archives
mapred.cache.archives.timestamps              mapreduce.job.cache.archives.timestamps
mapred.cache.files                            mapreduce.job.cache.files
mapred.cache.files.timestamps                 mapreduce.job.cache.files.timestamps
mapred.cache.localArchives                    mapreduce.job.cache.local.archives
mapred.cache.localFiles                       mapreduce.job.cache.local.files
mapred.child.tmp                              mapreduce.task.tmp.dir
mapred.cluster.average.blacklist.threshold    mapreduce.jobtracker.blacklist.average.threshold
mapred.cluster.map.memory.mb                  mapreduce.cluster.mapmemory.mb
mapred.cluster.max.map.memory.mb              mapreduce.jobtracker.maxmapmemory.mb
mapred.cluster.max.reduce.memory.mb           mapreduce.jobtracker.maxreducememory.mb
mapred.cluster.reduce.memory.mb               mapreduce.cluster.reducememory.mb
mapred.committer.job.setup.cleanup.needed     mapreduce.job.committer.setup.cleanup.needed
mapred.compress.map.output                    mapreduce.map.output.compress
mapred.create.symlink                         mapreduce.job.cache.symlink.create
mapred.data.field.separator                   mapreduce.fieldsel.data.field.separator
mapred.debug.out.lines                        mapreduce.task.debugout.lines
mapred.healthChecker.interval                 mapreduce.tasktracker.healthchecker.interval
mapred.healthChecker.script.args              mapreduce.tasktracker.healthchecker.script.args
mapred.healthChecker.script.path              mapreduce.tasktracker.healthchecker.script.path
mapred.healthChecker.script.timeout           mapreduce.tasktracker.healthchecker.script.timeout
mapred.heartbeats.in.second                   mapreduce.jobtracker.heartbeats.in.second
mapred.hosts                                  mapreduce.jobtracker.hosts.filename
mapred.hosts.exclude                          mapreduce.jobtracker.hosts.exclude.filename
mapred.inmem.merge.threshold                  mapreduce.reduce.merge.inmem.threshold
mapred.input.dir                              mapreduce.input.fileinputformat.inputdir
mapred.input.dir.formats                      mapreduce.input.multipleinputs.dir.formats
mapred.input.dir.mappers                      mapreduce.input.multipleinputs.dir.mappers
mapred.input.pathFilter.class                 mapreduce.input.pathFilter.class
mapred.jar                                    mapreduce.job.jar
mapred.job.classpath.archives                 mapreduce.job.classpath.archives
mapred.job.classpath.files                    mapreduce.job.classpath.files
mapred.job.id                                 mapreduce.job.id
mapred.job.map.memory.mb                      mapreduce.map.memory.mb
mapred.job.name                               mapreduce.job.name
mapred.job.priority                           mapreduce.job.priority
mapred.job.queue.name                         mapreduce.job.queuename
mapred.job.reduce.input.buffer.percent        mapreduce.reduce.input.buffer.percent
mapred.job.reduce.markreset.buffer.percent    mapreduce.reduce.markreset.buffer.percent
mapred.job.reduce.memory.mb                   mapreduce.reduce.memory.mb
mapred.job.reduce.total.mem.bytes             mapreduce.reduce.memory.totalbytes
mapred.job.reuse.jvm.num.tasks                mapreduce.job.jvm.numtasks
mapred.job.shuffle.input.buffer.percent       mapreduce.reduce.shuffle.input.buffer.percent
mapred.job.shuffle.merge.percent              mapreduce.reduce.shuffle.merge.percent
mapred.job.tracker                            mapreduce.jobtracker.address
mapred.job.tracker.handler.count              mapreduce.jobtracker.handler.count
mapred.job.tracker.http.address               mapreduce.jobtracker.http.address
mapred.job.tracker.jobhistory.lru.cache.size  mapreduce.jobtracker.jobhistory.lru.cache.size
mapred.job.tracker.persist.jobstatus.active   mapreduce.jobtracker.persist.jobstatus.active
mapred.job.tracker.persist.jobstatus.dir      mapreduce.jobtracker.persist.jobstatus.dir
mapred.job.tracker.persist.jobstatus.hours    mapreduce.jobtracker.persist.jobstatus.hours
mapred.job.tracker.retire.jobs                mapreduce.jobtracker.retirejobs
mapred.job.tracker.retiredjobs.cache.size     mapreduce.jobtracker.retiredjobs.cache.size
mapred.jobinit.threads                        mapreduce.jobtracker.jobinit.threads
mapred.jobtracker.instrumentation             mapreduce.jobtracker.instrumentation
mapred.jobtracker.job.history.block.size      mapreduce.jobtracker.jobhistory.block.size
mapred.jobtracker.maxtasks.per.job            mapreduce.jobtracker.maxtasks.perjob
mapred.jobtracker.restart.recover             mapreduce.jobtracker.restart.recover
mapred.jobtracker.taskScheduler               mapreduce.jobtracker.taskscheduler
mapred.jobtracker.taskalloc.capacitypad       mapreduce.jobtracker.taskscheduler.taskalloc.capacitypad
mapred.join.expr                              mapreduce.join.expr
mapred.join.keycomparator                     mapreduce.join.keycomparator
mapred.lazy.output.format                     mapreduce.output.lazyoutputformat.outputformat
mapred.line.input.format.linespermap          mapreduce.input.lineinputformat.linespermap
mapred.linerecordreader.maxlength             mapreduce.input.linerecordreader.line.maxlength
mapred.local.dir                              mapreduce.cluster.local.dir
mapred.local.dir.minspacekill                 mapreduce.tasktracker.local.dir.minspacekill
mapred.local.dir.minspacestart                mapreduce.tasktracker.local.dir.minspacestart
mapred.map.child.env                          mapreduce.map.env
mapred.map.child.java.opts                    mapreduce.map.java.opts
mapred.map.child.log.level                    mapreduce.map.log.level
mapred.map.child.ulimit                       mapreduce.map.ulimit
mapred.map.max.attempts                       mapreduce.map.maxattempts
mapred.map.output.compression.codec           mapreduce.map.output.compress.codec
mapred.map.task.debug.script                  mapreduce.map.debug.script
mapred.map.tasks                              mapreduce.job.maps
mapred.map.tasks.speculative.execution        mapreduce.map.speculative
mapred.mapoutput.key.class                    mapreduce.map.output.key.class
mapred.mapoutput.value.class                  mapreduce.map.output.value.class
mapred.mapper.regex                           mapreduce.mapper.regex
mapred.mapper.regex.group                     mapreduce.mapper.regexmapper..group
mapred.max.map.failures.percent               mapreduce.map.failures.maxpercent
mapred.max.reduce.failures.percent            mapreduce.reduce.failures.maxpercent
mapred.max.split.size                         mapreduce.input.fileinputformat.split.maxsize
mapred.max.tracker.blacklists                 mapreduce.jobtracker.tasktracker.maxblacklists
mapred.max.tracker.failures                   mapreduce.job.maxtaskfailures.per.tracker
mapred.merge.recordsBeforeProgress            mapreduce.task.merge.progress.records
mapred.min.split.size                         mapreduce.input.fileinputformat.split.minsize
mapred.min.split.size.per.node                mapreduce.input.fileinputformat.split.minsize.per.node
mapred.min.split.size.per.rack                mapreduce.input.fileinputformat.split.minsize.per.rack
mapred.output.compress                        mapreduce.output.fileoutputformat.compress
mapred.output.compression.codec               mapreduce.output.fileoutputformat.compress.codec
mapred.output.compression.type                mapreduce.output.fileoutputformat.compress.type
mapred.output.dir                             mapreduce.output.fileoutputformat.outputdir
mapred.output.key.class                       mapreduce.job.output.key.class
mapred.output.key.comparator.class            mapreduce.job.output.key.comparator.class
mapred.output.value.class                     mapreduce.job.output.value.class
mapred.output.value.groupfn.class             mapreduce.job.output.group.comparator.class
mapred.permissions.supergroup                 mapreduce.cluster.permissions.supergroup
mapred.pipes.user.inputformat                 mapreduce.pipes.inputformat
mapred.reduce.child.env                       mapreduce.reduce.env
mapred.reduce.child.java.opts                 mapreduce.reduce.java.opts
mapred.reduce.child.log.level                 mapreduce.reduce.log.level
mapred.reduce.child.ulimit                    mapreduce.reduce.ulimit
mapred.reduce.max.attempts                    mapreduce.reduce.maxattempts
mapred.reduce.parallel.copies                 mapreduce.reduce.shuffle.parallelcopies
mapred.reduce.slowstart.completed.maps        mapreduce.job.reduce.slowstart.completedmaps
mapred.reduce.task.debug.script               mapreduce.reduce.debug.script
mapred.reduce.tasks                           mapreduce.job.reduces
mapred.reduce.tasks.speculative.execution     mapreduce.reduce.speculative
mapred.seqbinary.output.key.class             mapreduce.output.seqbinaryoutputformat.key.class
mapred.seqbinary.output.value.class           mapreduce.output.seqbinaryoutputformat.value.class
mapred.shuffle.connect.timeout                mapreduce.reduce.shuffle.connect.timeout
mapred.shuffle.read.timeout                   mapreduce.reduce.shuffle.read.timeout
mapred.skip.attempts.to.start.skipping        mapreduce.task.skip.start.attempts
mapred.skip.map.auto.incr.proc.count          mapreduce.map.skip.proc-count.auto-incr
mapred.skip.map.max.skip.records              mapreduce.map.skip.maxrecords
mapred.skip.on                                mapreduce.job.skiprecords
mapred.skip.out.dir                           mapreduce.job.skip.outdir
mapred.skip.reduce.auto.incr.proc.count       mapreduce.reduce.skip.proc-count.auto-incr
mapred.skip.reduce.max.skip.groups            mapreduce.reduce.skip.maxgroups
mapred.speculative.execution.speculativeCap   mapreduce.job.speculative.speculativecap
mapred.submit.replication                     mapreduce.client.submit.file.replication
mapred.system.dir                             mapreduce.jobtracker.system.dir
mapred.task.cache.levels                      mapreduce.jobtracker.taskcache.levels
mapred.task.id                                mapreduce.task.attempt.id
mapred.task.is.map                            mapreduce.task.ismap
mapred.task.partition                         mapreduce.task.partition
mapred.task.profile                           mapreduce.task.profile
mapred.task.profile.maps                      mapreduce.task.profile.maps
mapred.task.profile.params                    mapreduce.task.profile.params
mapred.task.profile.reduces                   mapreduce.task.profile.reduces
mapred.task.timeout                           mapreduce.task.timeout
mapred.task.tracker.http.address              mapreduce.tasktracker.http.address
mapred.task.tracker.report.address            mapreduce.tasktracker.report.address
mapred.task.tracker.task-controller           mapreduce.tasktracker.taskcontroller
mapred.tasktracker.dns.interface              mapreduce.tasktracker.dns.interface
mapred.tasktracker.dns.nameserver             mapreduce.tasktracker.dns.nameserver
mapred.tasktracker.events.batchsize           mapreduce.tasktracker.events.batchsize
mapred.tasktracker.expiry.interval            mapreduce.jobtracker.expire.trackers.interval
mapred.tasktracker.indexcache.mb              mapreduce.tasktracker.indexcache.mb
mapred.tasktracker.instrumentation            mapreduce.tasktracker.instrumentation
mapred.tasktracker.map.tasks.maximum          mapreduce.tasktracker.map.tasks.maximum
mapred.tasktracker.memory_calculator_plugin   mapreduce.tasktracker.resourcecalculatorplugin
mapred.tasktracker.memorycalculatorplugin     mapreduce.tasktracker.resourcecalculatorplugin
mapred.tasktracker.reduce.tasks.maximum       mapreduce.tasktracker.reduce.tasks.maximum
mapred.temp.dir                               mapreduce.cluster.temp.dir
mapred.text.key.comparator.options            mapreduce.partition.keycomparator.options
mapred.text.key.partitioner.options           mapreduce.partition.keypartitioner.options
mapred.textoutputformat.separator             mapreduce.output.textoutputformat.separator
mapred.tip.id                                 mapreduce.task.id
mapred.used.genericoptionsparser              mapreduce.client.genericoptionsparser.used
mapred.userlog.limit.kb                       mapreduce.task.userlog.limit.kb
mapred.userlog.retain.hours                   mapreduce.job.userlog.retain.hours
mapred.work.output.dir                        mapreduce.task.output.dir
mapred.working.dir                            mapreduce.job.working.dir
mapreduce.combine.class                       mapreduce.job.combine.class
mapreduce.inputformat.class                   mapreduce.job.inputformat.class
mapreduce.jobtracker.permissions.supergroup   mapreduce.cluster.permissions.supergroup
mapreduce.map.class                           mapreduce.job.map.class
mapreduce.outputformat.class                  mapreduce.job.outputformat.class
mapreduce.partitioner.class                   mapreduce.job.partitioner.class
mapreduce.reduce.class                        mapreduce.job.reduce.class
min.num.spills.for.combine                    mapreduce.map.combine.minspills
reduce.output.key.value.fields.spec           mapreduce.fieldsel.reduce.output.key.value.fields.spec
sequencefile.filter.class                     mapreduce.input.sequencefileinputfilter.class
sequencefile.filter.frequency                 mapreduce.input.sequencefileinputfilter.frequency
sequencefile.filter.regex                     mapreduce.input.sequencefileinputfilter.regex
slave.host.name                               mapreduce.tasktracker.host.name
tasktracker.contention.tracking               mapreduce.tasktracker.contention.tracking
tasktracker.http.threads                      mapreduce.tasktracker.http.threads
user.name                                     mapreduce.job.user.name
============================================  ======

