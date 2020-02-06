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

**Executor resource calculation:**

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

Spark executor memory allocation layout and calculations:

spark.yarn.executor.MemoryOverhead = 3 \* 1024 = 3072

spark.executor.memory = 33 \* 1024 = 33792

spark.memory.fraction = 0.8 \* 34816 = 27852.8

spark.memory.storageFraction (cache, broadcast, accumulator) = 0.4 \* 34816 = 13926.4

User memory =  ( 1.0 - 0.8 ) \* 34816 = 6963.2

yarn.nodemanager.resource.memory-mb stays around = ~40GB


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

## Spark development setup using containers:

- Using Docker images: Setting up Kubernetes or Docker for SPARK, HDFS, YARN, Hue, Map-Reduce, HIVE and WebHCat development
- Using Kubernetes and HELM charts:
- Spark development environment setup using Helm charts using Kubernetes:
- Install Docker, Kubernetes and Helm in cluster and run following commands:

$ helm repo add bitnami https://charts.bitnami.com/bitnami

$ helm install bitnami/spark

\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_

Reference link:

- [https://www.ibm.com/support/knowledgecenter/en/SSCRJT\_5.0.4/com.ibm.swg.im.bigsql.doc/doc/bigsql\_TuneS3.html](https://www.ibm.com/support/knowledgecenter/en/SSCRJT_5.0.4/com.ibm.swg.im.bigsql.doc/doc/bigsql_TuneS3.html)
- [https://stackoverflow.com/questions/29964792/apache-hadoop-yarn-underutilization-of-cores](https://stackoverflow.com/questions/29964792/apache-hadoop-yarn-underutilization-of-cores)
- [https://developer.ibm.com/hadoop/2016/07/18/troubleshooting-and-tuning-spark-for-heavy-workloads/](https://stackoverflow.com/questions/29964792/apache-hadoop-yarn-underutilization-of-cores)
- [http://www.openkb.info/2019/04/what-is-difference-between.html](https://stackoverflow.com/questions/29964792/apache-hadoop-yarn-underutilization-of-cores)
- [https://spark.apache.org/docs/latest/tuning.html](https://stackoverflow.com/questions/29964792/apache-hadoop-yarn-underutilization-of-cores)
- [https://github.com/apache/spark/blob/79c66894296840cc4a5bf6c8718ecfd2b08bcca8/sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/BroadcastExchangeExec.scala#L104](https://stackoverflow.com/questions/29964792/apache-hadoop-yarn-underutilization-of-cores)
