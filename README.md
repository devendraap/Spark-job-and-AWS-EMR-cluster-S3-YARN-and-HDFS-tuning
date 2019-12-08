**Specs per CORE or TASK node of r4.12xlarge instance type:**

| Cores | 48 |
| --- | --- |
| Memory | (384GiB  \* 1000) / 1024 = 375 GB |

**Executor resource calculation:**

By assigning 1 core and 1GB for YARN, we are left with 47 core per node.

We allocated 5 cores per executor for max HDFS throughput

MemoryStore and BlockManagerMaster per node consumes 12GB per node

_Memory per executor = (374 - 12 -12) / 9 ~= 40 GB_

_Number of executor = (48 - 1) / 5 ~= 9 _

**Spark submit options:**

| **Parameter** | **Value** | **Explanation** | **Benefits** | **Reference** |
| --- | --- | --- | --- | --- |
| spark.executor.memory | 33g |   |   | Section 1.4. Spark executor memory |
| spark.executor.cores | 5 |   |   |   |
| spark.memory.fraction | 0.8 | Approx. (spark.memory.fraction \* spark.executor.memory) memory for task execution, shuffle, join, sort, aggregate  |   |   |
| spark.memory.storageFraction | 0.5 | Approx. (spark.memory.storageFraction \* spark.executor.memory) memory for cache, broadcast and accumulator |   |   |
| spark.executor.extraJavaOptions | -XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:OnOutOfMemoryError=&#39;kill -9 %p&#39; | The parameter -XX:+UseG1GC specifies that the G1GC garbage collector should be used. (The default is -XX:+UseParallelGC.) To understand the frequency and execution time of the garbage collection, use the parameters -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps. To initiate garbage collection sooner, set InitiatingHeapOccupancyPercent to 35 (the default is 0.45). Doing this helps avoid potential garbage collection for the total memory, which can take a significant amount of time. | Better garbage collection as G1 is suItable for large heap to resolve Out of memory issue, reduce the gc pause time, high latency and low throughput | [SPARK official docs](https://spark.apache.org/docs/latest/tuning.html) |
| spark.driver.maxResultSize | 20G | spark.sql.autoBroadcastJoinThreshold \&lt; spark.driver.maxResultSize \&lt; spark.driver.memory | Resolves error: serialized results of x tasks is bigger than spark.driver.maxResultSize |   |
| spark.yarn.maxAppAttempts | 2 | Maximum attempts for running application |   |   |
| spark.rpc.message.maxSize | 2048 | Increases remote procedure call message size | Resolves error: exceeds max allowed: spark.rpc.message.maxSize |   |
| spark.spark.worker.timeout | 240 | Allows task working on skewed data more time for execution. Proper re-partitioning (with salting) on join or groupBy column reduces time for execution | Lost executor xx on slave1.cluster: Executor heartbeat timed out after xxxxx ms |   |
| spark.network.timeout | 9999s |   |   |   |
| spark.shuffle.file.buffer | 1024k | Reduce the number of times the disk file overflows during the shuffle write process, which can reduce the number of disk IO times and improve performance |   |   |
| spark.locality.wait | 15s | Reduces large amounts of data transfer over network (shuffling) |   |   |
| spark.shuffle.io.connectionTimeout | 3000 |   | Resolves error: &quot;org.apache.spark.rpc.RpcTimeoutException: Futures timed out after [120 seconds]&quot; | [IBM: Spark heavy workloads tuning](https://developer.ibm.com/hadoop/2016/07/18/troubleshooting-and-tuning-spark-for-heavy-workloads/) |
| spark.shuffle.io.retryWait | 60s |   | Resolves error: org.apache.spark.shuffle.MetadataFetchFailedException: Missing an output location for shuffle 0 |   |
| spark.reducer.maxReqsInFlight | 1 |   |   |   |
| spark.shuffle.io.maxRetries | 10 |   |   |   |
| spark.scheduler.maxRegisteredResourcesWaitingTime | 180s | The maximum amount of time it will wait before scheduling begins is controlled | Resolves error: Application\_xxxxx\_xxx failed 2 times due to AM container for appattempt\_xxxx\_xxxxx. Exception from container-launch. |   |
| spark.dynamicAllocation.enabled | TRUE |   |   |   |
| spark.dynamicAllocation.executorIdleTimeout | 60s | Remove executor with if idle for more than this duration |   |   |
| spark.dynamicAllocation.cachedExecutorIdleTimeout | 36000s | Remove executor with cached data blocks if idle for more than this duration |   |   |
| spark.sql.broadcastTimeout | 720000 | Timeout in seconds for the broadcast wait time in broadcast joins | Resolves error: ERROR yarn.ApplicationMaster: User class threw exception: java.util.concurrent.TimeoutException: Futures timed out after |   |
| spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version | 2 | Major difference between mapreduce.fileoutputcommitter.algorithm.version=1 and 2 is : | Allows reducers to do mergePaths() to move those files to the final output directory | [http://www.openkb.info/2019/04/what-is-difference-between.html](http://www.openkb.info/2019/04/what-is-difference-between.html) |
|   |   | Either AM or Reducers will do the mergePaths(). |   |   |
| spark.sql.autoBroadcastJoinThreshold | 0 | Maximum broadcast table is limited by spark default i.e 8gb |   | [Github source code](https://github.com/apache/spark/blob/79c66894296840cc4a5bf6c8718ecfd2b08bcca8/sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/BroadcastExchangeExec.scala#L104) |
| spark.sql.inMemoryColumnarStorage.compressed | TRUE | Enables compression. Reduce network IO and memory usage using spark compression default codec lz4 |   |   |
| spark.rdd.compress | TRUE |   |   |   |
| spark.shuffle.compress | TRUE |   |   |   |
| spark.shuffle.spill.compress | TRUE |   |   |   |
| spark.checkpoint.compress | TRUE |   |   |   |
| spark.broadcast.compress | TRUE |   |   |   |
| spark.storage.level | MEMORY\_AND\_DISK\_SER | Spill partitions that don&#39;t fit in executor memory. Uses low space (i.e. memory in RAM or storage in SSD) |   | [StackOverflow](https://stackoverflow.com/questions/30520428/what-is-the-difference-between-memory-only-and-memory-and-disk-caching-level-in) |
| spark.serializer | org.apache.spark.serializer.KryoSerializer | Better than default spark serializer |   |   |
| spark.hadoop.s3.multipart.committer.conflict-mode | replace | Setting for new Hadoop parquet magic committer |   |   |
| spark.shuffle.consolidateFiles | TRUE | Optimization for custom ShuffleHash join implementation. Note that the MergeSort join is default method which is better for large datasets due to memory limitation |   |   |
| spark.reducer.maxSizeInFlight | 96 | Increase data reducers is requested from &quot;map&quot; task outputs in bigger chunks which would improve performance |   |   |
| spark.kryoserializer.buffer.max | 1024m |   | Resolves error: com.esotericsoftware.kryo.KryoException: Buffer overflow. Available: 0, required: 57197 |   |
| spark.sql.shuffle.partitions | 10000 | Number of partitions during join operation |   |   |
| spark.sql.files.maxPartitionBytes | 134217728 | Reparation file after reading to 128MB each |   |   |
| spark.scheduler.listenerbus.eventqueue.capacity | 20000 | Resolves error: ERROR scheduler.LiveListenerBus: Dropping SparkListenerEvent because no remaining room in event queue. This likely means one of the SparkListeners is too slow and cannot keep up with the rate at which tasks are being started by the scheduler |   |   |

**Spark executor memory layout:**

| spark.yarn.executor.MemoryOverhead | 3 \* 1024 = 3072 |
| --- | --- |
| spark.executor.memory | 33 \* 1024 = 33792 |
| spark.memory.fraction | 0.8 \* 34816 = 27852.8 |
| spark.memory.storageFraction(cache, broadcast, accumulator) | 0.4 \* 34816 = 13926.4 |
| User memory | ( 1.0 - 0.8 ) \* 34816 = 6963.2 |
| yarn.nodemanager.resource.memory-mb stays around | ~40GB |

**EMR cluster tuning:**

| Classification | Configuration Properties | Value | Usage | Reference |
| --- | --- | --- | --- | --- |
| core-site | fs.s3a.server-side-encryption-algorithm | AES256 | Enables S3 AES256 data encryption |   |
| yarn-site | yarn.log-aggregation.retain-seconds | -1 |   |   |
|   |   yarn.nodemanager.vmem-check-enabled | false | To disable hard memory restriction causing OOM (out of memory) JVM error(Note: Re-partition data in job based on size) |   |
|   | yarn.nodemanager.pmem-check-enabled | false |   |   |
|   | yarn.nm.liveness-monitor.expiry-interval-ms | 360000 | Increases time to wait until a node manager is considered dead |   |
|   | yarn.resourcemanager.decommissioning.timeout | 3600 | Increases timeout interval to blacklist node |   |
|   | yarn.log-aggregation-enable | true | Aggregates logs at driver node |   |
| hdfs-site | dfs.replication | 2 | HDFS data replication factor for EMR with auto scaling enabled for core nodes |   |
| capacity-scheduler | yarn.scheduler.capacity.resource-calculator | org.apache.hadoop.yarn.util.resource.DominantResourceCalculator | The default resource calculator i.e org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator uses only memory information for allocating containers and CPU scheduling is not enabled by default | [Stackoverflow](https://stackoverflow.com/questions/29964792/apache-hadoop-yarn-underutilization-of-cores) |
|   | yarn.scheduler.capacity.root.default.maximum-capacity | 100 | Uses all resources of dedicated cluster |   |
|   | yarn.scheduler.capacity.root.default.capacity | 100 |   |   |
| emrfs-site |     fs.s3.enableServerSideEncryption | true | Enables S3 AES256 data encryption |   |
|   | fs.s3a.attempts.maximum | 100 | Workaround to resolve S3&#39;s storage eventual consistency missing file error due to replication in multiple AZ (availability zone) |   |
|   | fs.s3a.committer.magic.enabled | true | Setting for new Hadoop parquet magic committer |   |
|   | fs.s3a.connection.maximum | 250 | Increases S3 IO speed | [IBM article](https://www.ibm.com/support/knowledgecenter/en/SSCRJT_5.0.4/com.ibm.swg.im.bigsql.doc/doc/bigsql_TuneS3.html) |
|   | fs.s3a.threads.core | 250 |   |   |
|   | fs.s3a.fast.upload | true |   |   |
