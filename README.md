We will process our data in AWS environment using Spark (on EMR) and use object storage (S3) for storage.

We should prefer Dataset for storing and processing data in memory because of following reasons:

- Static-typing and runtime type-safety.  With a DataFrame, you can select a nonexistent column and notice your mistake only when you run your code. With a Dataset, you have a compile time error.
- Provides Catalyst optimization and benefit from Tungsten&#39;s efficient bytecode generation due to Encoders used for Dataset
- Dataset has helpers called encoders, which are smart and efficient encoding utilities that convert data inside each user-defined object into a compact binary format. Spark understands the structure of data in Datasets, it can create a more optimal layout in memory when caching Datasets. This translates into a reduction of memory usage if and when a Dataset is cached in memory as well as a reduction in the number of bytes that Spark needs to transfer over a network during the shuffling process.
- Kryo serializer usage leads to Spark storing every row in the Dataset as a flat binary object using Spark&#39;s internal encoders and is \&gt;10x faster than dataframe&#39;s Kryo serialization.

Ref: Heather Miller&#39;s Course

While processing billions of records or TB&#39;s of data we faced multiple hurdles. This wiki documents extensively the error team faced while processing large dataset using Spark jobs and how to resolve them.

The Spark job and cluster optimization for processing large dataset are also explained below.

**Note: Please go through reference links provided to fully understand how spark options affects the data processing**

# Executor resource calculation:

By assigning 1 core and 1GB for YARN, we are left with 47 core per node.

We allocated 5 cores per executor for max HDFS throughput

MemoryStore and BlockManagerMaster per node consumes 12GB per node

- Memory per executor = (374 - 12 -12) / 9 ~= 40 GB
- Number of executor = (48 - 1) / 5 ~= 9

Specs per CORE or TASK node of r4.12xlarge instance type:

- Cores = 48
- Memory = (384 GiB  \* 1000) / 1024 = 375 GB

Note: If EMR cluster is configured to use task nodes, do not exceed CORE Node to TASK Node ratio 2:1 (as task node does not have HDFS storage. Also, allocate more HDFS storage to compensate for the lack of HDFS storage on task nodes).

** **

# Spark submit options:

**Spark executor memory allocation layout and calculations:**

spark.yarn.executor.MemoryOverhead = 3 \* 1024 = 3072

spark.executor.memory = 33 \* 1024 = 33792

spark.memory.fraction = 0.8 \* 34816 = 27852.8

spark.memory.storageFraction (cache, broadcast, accumulator) = 0.4 \* 34816 = 13926.4

User memory =  ( 1.0 - 0.8 ) \* 34816 = 6963.2

yarn.nodemanager.resource.memory-mb stays around = ~40GB


| Parameter | spark.executor.memory |
| --- | --- |
| Value | 33g |
| Explanation |   |
| Benefits |   |
| Reference | [Link](https://wiki.advisory.com/display/LD/Spark+job+and+AWS+EMR+cluster+%28S3%2C+YARN+and+HDFS%29+tuning#SparkjobandAWSEMRcluster(S3,YARNandHDFS)tuning-Spark-executor-memory-layout) |

| Parameter | spark.executor.cores |
| --- | --- |
| Value | 5 |
| Explanation |   |
| Benefits |   |
| Reference |   |

| Parameter | spark.memory.fraction |
| --- | --- |
| Value | 0.8 |
| Explanation | Approx. (spark.memory.fraction \* spark.executor.memory) memory for task execution, shuffle, join, sort, aggregate |
| Benefits |   |
| Reference |   |

| Parameter | spark.memory. storageFraction |
| --- | --- |
| Value | 0.5 |
| Explanation | Approx. (spark.memory. storageFraction \* spark.executor. memory) memory for cache, broadcast and accumulator |
| Benefits |   |
| Reference |   |

| Parameter | spark.dynamicAllocation. enabled and |
| --- | --- |
| Value | TRUE |
| Explanation | To allocate executor dynamically based on yarn.scheduler. capacity.resource- calculator = org.apache. hadoop.yarn. util.resource. DominantResourceCalculator |
| Benefits | Scales number of executors based on CPU and memory requirements. |
| Reference | [Link](https://stackoverflow.com/questions/55925106/exceptions-while-running-spark-job-on-emr-cluster-java-io-ioexception-all-data/55925309#55925309) |

| Parameter | spark.shuffle. service.enabled |
| --- | --- |
| Value | TRUE |
| Explanation | Spark shuffle service maintains the shuffle files generated by all Spark executors that ran on that node. Spark executors write the shuffle data and manage it |
| Benefits | Spark shuffle service service preserves the shuffle files written by executors so the executors can be safely removedResolves error: java.io.IOException: All datanodes are bad.&quot; |
| Reference |   |

| Parameter | spark.executor.extraJavaOptions |
| --- | --- |
| Value | -XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:OnOutOfMemoryError=&#39;kill -9 %p&#39; |
| Explanation | The parameter -XX:+UseG1GC specifies that the G1GC garbage collector should be used. (The default is -XX:+UseParallelGC.) To understand the frequency and execution time of the garbage collection, use the parameters -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps. To initiate garbage collection sooner, set InitiatingHeapOccupancyPercent to 35 (the default is 0.45). Doing this helps avoid potential garbage collection for the total memory, which can take a significant amount of time. |
| Benefits | Better garbage collection as G1 is suItable for large heap to resolve Out of memory issue, reduce the gc pause time, high latency and low throughput |
| Reference | [Link](https://spark.apache.org/docs/latest/tuning.html) |

| Parameter | spark.driver. maxResultSize |
| --- | --- |
| Value | 20G |
| Explanation | spark.sql.autoBroadcastJoinThreshold \&lt; spark.driver.maxResultSize \&lt; spark.driver.memory |
| Benefits | Resolves error: serialized results of x tasks is bigger than spark.driver.maxResultSize |
| Reference |   |

| Parameter | spark.yarn. maxAppAttempts |
| --- | --- |
| Value | 2 |
| Explanation | Maximum attempts for running application |
| Benefits |   |
| Reference |   |

| Parameter | spark.rpc. message.maxSize |
| --- | --- |
| Value | 2048 |
| Explanation | Increases remote procedure call message size |
| Benefits | Resolves error: exceeds max allowed: spark.rpc.message.maxSize |
| Reference |   |

| Parameter | spark.spark. worker.timeout |
| --- | --- |
| Value | 240 |
| Explanation | Allows task working on skewed data more time for execution. Proper re-partitioning (with salting) on join or groupBy column reduces time for execution |
| Benefits | Resolves:  Lost executor xx on slave1.cluster: Executor heartbeat timed out after xxxxx msWARN TransportChannelHandler: Exception in connection from /172.31.3.245:46014 |
| Reference | [Link](https://stackoverflow.com/questions/39347392/how-to-fix-connection-reset-by-peer-message-from-apache-spark) |

| Parameter | spark.network. timeout |
| --- | --- |
| Value | 9999s |
| Explanation |   |
| Benefits | java.io.IOException: Connection reset by peer |
| Reference |   |

| Parameter | spark.shuffle. file.buffer |
| --- | --- |
| Value | 1024k |
| Explanation | Reduce the number of times the disk file overflows during the shuffle write process, which can reduce the number of disk IO times and improve performance |
| Benefits |   |
| Reference |   |

| Parameter | spark.locality. wait |
| --- | --- |
| Value | 15s |
| Explanation | Reduces large amounts of data transfer over network (shuffling) |
| Benefits |   |
| Reference |   |

| Parameter | spark.shuffle. io.connectionTimeout |
| --- | --- |
| Value | 3000 |
| Explanation |   |
| Benefits | Resolves error: &quot;org.apache.spark.rpc.RpcTimeoutException: Futures timed out after [120 seconds]&quot; |
| Reference | [Link](https://developer.ibm.com/hadoop/2016/07/18/troubleshooting-and-tuning-spark-for-heavy-workloads/) |

| Parameter | spark.shuffle. io.retryWait |
| --- | --- |
| Value | 60s |
| Explanation |   |
| Benefits | Resolves error: org.apache.spark.shuffle.MetadataFetchFailedException: Missing an output location for shuffle 1 |
| Reference |   |

| Parameter | spark.reducer. maxReqsInFlight |
| --- | --- |
| Value | 1 |
| Explanation |   |
| Benefits |   |
| Reference |   |

| Parameter | spark.shuffle. io.maxRetries |
| --- | --- |
| Value | 10 |
| Explanation |   |
| Benefits |   |
| Reference |   |

| Parameter | spark.scheduler. maxRegisteredResourcesWaitingTime |
| --- | --- |
| Value | 180s |
| Explanation | The maximum amount of time it will wait before scheduling begins is controlled |
| Benefits | Resolves error: Application\_xxxxx\_xxx failed 2 times due to AM container for appattempt\_xxxx\_xxxxx. Exception from container-launch. |
| Reference |   |

| Parameter | spark.dynamicAllocation. enabled |
| --- | --- |
| Value | TRUE |
| Explanation |   |
| Benefits |   |
| Reference |   |

| Parameter | spark.dynamicAllocation. executorIdleTimeout |
| --- | --- |
| Value | 60s |
| Explanation | Remove executor with if idle for more than this duration |
| Benefits |   |
| Reference |   |

| Parameter | spark.dynamicAllocation. cachedExecutorIdleTimeout |
| --- | --- |
| Value | 36000s |
| Explanation | Remove executor with cached data blocks if idle for more than this duration |
| Benefits |   |
| Reference |   |

| Parameter | spark.sql. broadcastTimeout |
| --- | --- |
| Value | 72000 |
| Explanation | Timeout in seconds for the broadcast wait time in broadcast joins |
| Benefits | Resolves error: ERROR yarn.ApplicationMaster: User class threw exception: java.util.concurrent.TimeoutException: Futures timed out after |
| Reference |   |

| Parameter | spark.hadoop. mapreduce.fileoutputcommitter. algorithm.version |
| --- | --- |
| Value | 2 |
| Explanation | Major difference between mapreduce.fileoutputcommitter.algorithm.version=1 and 2 is : Either AM or Reducers will do the mergePaths(). |
| Benefits | Allows reducers to do mergePaths() to move those files to the final output directory |
| Reference | [Link](http://www.openkb.info/2019/04/what-is-difference-between.html) |

| Parameter | spark.sql. autoBroadcastJoinThreshold |
| --- | --- |
| Value | 0 |
| Explanation | Maximum broadcast table is limited by spark default i.e 8gb |
| Benefits |   |
| Reference | [Link](https://github.com/apache/spark/blob/79c66894296840cc4a5bf6c8718ecfd2b08bcca8/sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/BroadcastExchangeExec.scala#L104) |

| Parameter | spark.io. compression.codec |
| --- | --- |
| Value | zstd |
| Explanation | Reduces serialized data size by 50% resulting in less spill size (memory and disk), storage io and network io, but increases CPU overhead by 2-5% which is acceptable while processing large datasets |
| Benefits | Used by spark.sql.inMemoryColumnarStorage.compressed, spark.rdd.compress, spark.shuffle.compress, spark.shuffle.compress, spark.shuffle.spill.compress, spark.checkpoint.compress, spark.broadcast.compress.Which allows us to broadcast table with 2x records, spill less size (memory and data), reduce disk and network io. |
| Reference | [Link](https://docs.aws.amazon.com/redshift/latest/dg/zstd-encoding.html) |

| Parameter | spark.io. compression.zstd. level |
| --- | --- |
| Value | 6 |
| Explanation |   |
| Benefits |   |
| Reference |   |

| Parameter | spark.sql. inMemoryColumnarStorage. compressed |
| --- | --- |
| Value | TRUE |
| Explanation | Enables compression. Reduce network IO and memory usage using spark compression codec spark.io.compression.codec |
| Benefits |   |
| Reference |   |

| Parameter | spark.rdd. compress |
| --- | --- |
| Value | TRUE |
| Explanation |   |
| Benefits |   |
| Reference |   |

| Parameter | spark.shuffle. compress |
| --- | --- |
| Value | TRUE |
| Explanation |   |
| Benefits |   |
| Reference |   |

| Parameter | spark.shuffle. spill.compress |
| --- | --- |
| Value | TRUE |
| Explanation |   |
| Benefits |   |
| Reference |   |

| Parameter | spark.checkpoint. compress |
| --- | --- |
| Value | TRUE |
| Explanation |   |
| Benefits |   |
| Reference |   |

| Parameter | spark.broadcast. compress |
| --- | --- |
| Value | TRUE |
| Explanation |   |
| Benefits |   |
| Reference |   |

| Parameter | spark.storage. level |
| --- | --- |
| Value | MEMORY\_AND\_DISK\_SER |
| Explanation | Spill partitions that don&#39;t fit in executor memory. Uses low space (i.e. memory in RAM or storage in SSD) |
| Benefits |   |
| Reference | StackOverflow |

| Parameter | spark.serializer |
| --- | --- |
| Value | org.apache.spark.serializer.KryoSerializer |
| Explanation | Better than default spark serializer |
| Benefits |   |
| Reference |   |

| Parameter | spark.hadoop.s3. multipart.committer. conflict-mode |
| --- | --- |
| Value | replace |
| Explanation | Setting for new Hadoop parquet magic committer |
| Benefits |   |
| Reference |   |

| Parameter | spark.shuffle. consolidateFiles |
| --- | --- |
| Value | TRUE |
| Explanation | Optimization for custom ShuffleHash join implementation. Note that the MergeSort join is default method which is better for large datasets due to memory limitation |
| Benefits |   |
| Reference |   |

| Parameter | spark.reducer. maxSizeInFlight |
| --- | --- |
| Value | 96 |
| Explanation | Increase data reducers is requested from &quot;map&quot; task outputs in bigger chunks which would improve performance |
| Benefits |   |
| Reference |   |

| Parameter | spark.kryoserializer. buffer.max |
| --- | --- |
| Value | 1024m |
| Explanation |   |
| Benefits | Resolves error: com.esotericsoftware. kryo.KryoException: Buffer overflow. Available: 0, required: 57197 |
| Reference |   |

| Parameter | spark.sql.shuffle.partitions |
| --- | --- |
| Value | 10000 |
| Explanation | Number of partitions during join operation |
| Benefits |   |
| Reference |   |

| Parameter | spark.sql. files.maxPartitionBytes |
| --- | --- |
| Value | 134217728 |
| Explanation | Reparation file after reading to 128MB each |
| Benefits |   |
| Reference |   |

| Parameter | spark.scheduler. listenerbus.eventqueue. capacity |
| --- | --- |
| Value | 20000 |
| Explanation | Resolves error: ERROR scheduler.LiveListenerBus: Dropping SparkListenerEvent because no remaining room in event queue. This likely means one of the SparkListeners is too slow and cannot keep up with the rate at which tasks are being started by the scheduler |

# Data based spark optimizations:

Let&#39;s assume we have two tables whose raw/csv file size is 3TB and 500GB respectively and needs to be joined on particular columns. Following are the ways to optimize the joins and prevent the job failures as the data grows gradually after each refresh.

1. Set spark.sql.files.maxPartitionBytes to 128MB which will reparation the files after reading so that resultant partitions will be each of 128MB.
2. If fill rate of joining column is not 100%, filter records containing null and perform join on those records. Union the output with records containing null values.
3. Set spark.shuffle.paritions value to re-partition data and increase tasks during join operation resulting in increased parallel processing.  The partition size should be ~128MB corresponding to the block size in EMRFS (ref. AWS docs).
4. To know amount of data processed and time taken by each task, open the stage summary metrics in Application Master:
5. If 25th percentile takes \&lt;100ms, but MAX time is \&gt; 5 min for task implies that the data is skewed. The data can be evenly distributed by adding salt column:

    import org.apache.spark.sql.functions.\_ 
    df.withColumn(&quot;salt&quot;, (rand \* n).cast(IntegerType))
    .groupBy(&quot;salt&quot;, groupByFields)
    .agg(aggFields)
    .groupBy(groupByFields)
    .agg(aggFields)

1. If the MAX time taken is greater than 5 min for a task, try increasing partition size.
2. If few tasks are taking too long to execute then you are performing cartesian joins due to NULL or repeated values in column used to join.
3. If data processed in only few ETL steps is too large try following options:
4. Turn on auto-scaling option of cluster to allocate more core nodes while processing those few large data processing ETL steps.
5. Partition the larger table by column which can evenly distribute the records (like year or quarter) and persist it to EMRFS.
6. Read each partition at a time by using filter, perform join and write output to EMRFS.

# EMR cluster tuning:


| Configuration Properties | dfs.replication |
| --- | --- |
| Classification | hdfs-site |
| Value | 2 |
| Usage | HDFS data replication factor for EMR with auto scaling enabled for core nodes |
| Reference | (blank) |

| Configuration Properties | fs.s3.enableServerSideEncryption |
| --- | --- |
| Classification | emrfs-site |
| Value | TRUE |
| Usage | Enables S3 AES256 data encryption |
| Reference | (blank) |

| Configuration Properties | fs.s3a.attempts.maximum |
| --- | --- |
| Classification | emrfs-site |
| Value | 100 |
| Usage | Workaround to resolve S3&#39;s storage eventual consistency missing file error due to replication in multiple AZ (availability zone) |
| Reference | (blank) |

| Configuration Properties | fs.s3a.committer.magic.enabled |
| --- | --- |
| Classification | emrfs-site |
| Value | TRUE |
| Usage | Setting for new Hadoop parquet magic committer |
| Reference | (blank) |

| Configuration Properties | fs.s3a.connection.maximum |
| --- | --- |
| Classification | emrfs-site |
| Value | 250 |
| Usage | Increases S3 IO speed |
| Reference | https://www.ibm.com/support/knowledgecenter/en/SSCRJT\_5.0.4/com.ibm.swg.im.bigsql.doc/doc/bigsql\_TuneS3.html |

| Configuration Properties | fs.s3a.fast.upload |
| --- | --- |
| Classification | emrfs-site |
| Value | TRUE |
| Usage | Increases S3 IO speed |
| Reference | https://www.ibm.com/support/knowledgecenter/en/SSCRJT\_5.0.4/com.ibm.swg.im.bigsql.doc/doc/bigsql\_TuneS3.html |

| Configuration Properties | fs.s3a.server-side-encryption-algorithm |
| --- | --- |
| Classification | core-site |
| Value | AES256 |
| Usage | Enables S3 AES256 data encryption |
| Reference | (blank) |

| Configuration Properties | fs.s3a.threads.core |
| --- | --- |
| Classification | emrfs-site |
| Value | 250 |
| Usage | Increases S3 IO speed |
| Reference | https://www.ibm.com/support/knowledgecenter/en/SSCRJT\_5.0.4/com.ibm.swg.im.bigsql.doc/doc/bigsql\_TuneS3.html |

| Configuration Properties | yarn.log-aggregation.retain-seconds |
| --- | --- |
| Classification | yarn-site |
| Value | -1 |
| Usage | (blank) |
| Reference | (blank) |

| Configuration Properties | yarn.log-aggregation-enable |
| --- | --- |
| Classification | yarn-site |
| Value | TRUE |
| Usage | Aggregates logs at driver node |
| Reference | (blank) |

| Configuration Properties | yarn.nm.liveness-monitor.expiry-interval-ms |
| --- | --- |
| Classification | yarn-site |
| Value | 360000 |
| Usage | Increases time to wait until a node manager is considered dead |
| Reference | (blank) |

| Configuration Properties | yarn.nodemanager.pmem-check-enabled |
| --- | --- |
| Classification | yarn-site |
| Value | FALSE |
| Usage | (Note: Re-partition data in job based on size) |
| Reference | (blank) |

| Configuration Properties | yarn.nodemanager.vmem-check-enabled |
| --- | --- |
| Classification | yarn-site |
| Value | FALSE |
| Usage | To disable hard memory restriction causing OOM (out of memory) JVM error |
| Reference | (blank) |

| Configuration Properties | yarn.resourcemanager.decommissioning.timeout |
| --- | --- |
| Classification | yarn-site |
| Value | 3600 |
| Usage | Increases timeout interval to blacklist node |
| Reference | (blank) |

| Configuration Properties | yarn.scheduler.capacity.resource-calculator |
| --- | --- |
| Classification | capacity-scheduler |
| Value | org.apache.hadoop.yarn.util.resource.DominantResourceCalculator |
| Usage | The default resource calculator i.e org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator uses only memory information for allocating containers and CPU scheduling is not enabled by default |
| Reference | https://stackoverflow.com/questions/29964792/apache-hadoop-yarn-underutilization-of-cores |

| Configuration Properties | yarn.scheduler.capacity.root.default.capacity |
| --- | --- |
| Classification | capacity-scheduler |
| Value | 100 |
| Usage | Uses all resources of dedicated cluster |
| Reference | (blank) |

| Configuration Properties | yarn.scheduler.capacity.root.default.maximum-capacity |
| --- | --- |
| Classification | capacity-scheduler |
| Value | 100 |
| Usage | Uses all resources of dedicated cluster |
| Reference | (blank) |

## Spark development setup using containers:

- Using Docker images: Setting up Kubernetes or Docker for SPARK, HDFS, YARN, Hue, Map-Reduce, HIVE and WebHCat development
- Using Kubernetes and HELM charts:
- Spark development environment setup using Helm charts using Kubernetes:
- Install Docker, Kubernetes and Helm in cluster and run following commands:

$ helm repo add bitnami https://charts.bitnami.com/bitnami

$ helm install bitnami/spark

\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_

Reference link:

- [Link](https://www.ibm.com/support/knowledgecenter/en/SSCRJT_5.0.4/com.ibm.swg.im.bigsql.doc/doc/bigsql_TuneS3.html)
- [Link](https://stackoverflow.com/questions/29964792/apache-hadoop-yarn-underutilization-of-cores)
- [Link](https://stackoverflow.com/questions/29964792/apache-hadoop-yarn-underutilization-of-cores)
- [Link](https://stackoverflow.com/questions/29964792/apache-hadoop-yarn-underutilization-of-cores)
- [Link](https://stackoverflow.com/questions/29964792/apache-hadoop-yarn-underutilization-of-cores)
- [Link](https://stackoverflow.com/questions/29964792/apache-hadoop-yarn-underutilization-of-cores)
