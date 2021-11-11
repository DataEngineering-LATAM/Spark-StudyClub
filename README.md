# Spark

- Architecture
    
    [Cluster Mode Overview](https://spark.apache.org/docs/latest/cluster-overview.html)
    
    - Application
        - User program built on Spark. *Consists of a driver program and executors on the cluster*.
    - Cluster
        - The system currently supports several cluster managers:
            - Standalone – a simple cluster manager included with Spark that makes it easy to set up a cluster.
            - Apache Mesos – a general cluster manager that can also run Hadoop MapReduce and service applications.
            - Hadoop YARN – the resource manager in Hadoop 2.
            - Kubernetes – an open-source system for automating deployment, scaling, and management of containerized applications.
        - Groups of nodes
        - Nodes are the individual machines within a cluster (generally a VM)
        - Databricks, the Driver (a JVM) and each executor (each a JVM) all run in their own nodes
        
        Spark Cluster → Driver + Executors
        
    - Cluster manager
        - An external service for acquiring resources on the cluster (e.g. standalone manager, Mesos, YARN)
    - Driver
        - Run the Spark applications
        - Assigns tasks to slots in a executor
        - coordinates the work between tasks
        - Receives the results, if any
    - Executor
        - JVM in a node
        - Provides an environment in which tasks can be run
        - Leverages the JVM to execute many threads (4 cores = 4 slots = 4 threads)
    - Jobs
        - A parallel computation consisting of multiple tasks that gets spawned in response to a Spark action (e.g. save, collect);
    - Stage
        - Each job gets divided into smaller sets of tasks called stages that depend on each other (similar to the map and reduce stages in MapReduce)
        - A stage cannot be completed until all tasks are completed
        - One long-running task can delay an entire stage from completing
    - Task/Cores/Threads
        - The lowest unit of parallelization
        - Executes a set of transformations against a partition as directed to by the driver
        - A **Task** is a single operation (`.map` or `.filter`) applied to a single **Partition**.
        - Each **Task** is executed as a single thread in an **Executor**!
        - If your dataset has 2 **Partitions**, an operation such as a `filter()` will trigger 2 **Tasks**, one for each **Partition**.
        - Slots
            
            Cores (or slots) are the number of available threads for each executor
            
            <aside>
            💡 Slots indicate threads available to perform parallel work for Spark. Spark documentation often refers to these threads as cores, which is a confusing term, as the number of slots available on a particular machine does not necessarily have any relationship to the number of physical CPU cores on that machine.
            
            </aside>
            
    - Partition
        - A ~128MB chunk of the large dataset
        - Each task processes one and only one partition
        - The size and record splits are decided by the Driver
        - The initial size is partially adjustable with various configuration option
    
    ![Spark%20dd8609c5ca3041bb89b503bc314efe53/Untitled.png](Spark%20dd8609c5ca3041bb89b503bc314efe53/Untitled.png)
    
- DataFrames
    - Reader & Writer
        
        format("parquet") is by default in Spark, format("delta") is by default databricks
        
        ```scala
        val parqDF = spark.read.parquet("/../output/people.parquet")
        val parqDF = spark.read.load("/../output/people.parquet")
        val parqDF = spark.read.format("parquet").load("/../people.parquet")
        
        // Read a specific Parquet partition
        val parqDF = spark.read.parquet("/../people2.parquet/gender=M")
        
        //WRITER
        df.write.parquet("/tmp/output/people.parquet")
        df.write.mode("append").option("compression", "snappy").parquet("/../people.parquet")
        df.write.partitionBy("gender","salary").parquet("/../../people2.parquet")
        
        df.write.format("parquet").save("/tmp/output/people.parquet")
        df.write.save("/tmp/output/people.parquet")
        df.write.format("parquet").mode("append").option("compression", "snappy").save("/../people.parquet")
        df.write.format("parquet").partitionBy("gender","salary").save("/../../people2.parquet")
        ```
        
        ```scala
        // Read one file  or directory
        val df = spark.read.csv("./zipcodes.csv")
        val df = spark.read.format("csv").load("./zipcodes.csv")
        
        // Read multiple files
        val df = spark.read.csv("path1,path2,path3")
        
        //Read multi options
        val df = spark.read.option("sep", "\t")
          .option("header", true)
          .option("inferSchema", true) // .schema(userDefinedSchema)
          .csv(usersCsvPath)
        
        ```
        
        ```scala
        //read json file into dataframe
        val df = spark.read.json("./zipcodes.json")
        val df = spark.read.format("json").load("./zipcodes.json")
        
        //read multiline json file
        val multiline_df = spark.read.option("multiline","true").json("./multiline.json")
        
        // Read multiple files
        val df = spark.read.json("path1,path2,path3")
        
        //WRITING
        df.write.format("csv").mode("overwrite").option("header","true").save("/tmp/output/df_csv")
        ```
        
        ```scala
        df.write.mode("overwrite").saveAsTable("TableName")
        ```
        
        ```sql
        CREATE OR REPLACE TEMPORARY VIEW my_view_name
        	USING parquet
        	OPTION (path "/../../../file.parquet")
        ```
        
         Best Practice: Write Results to a Delta Table
        
        ```scala
        eventsDF.write.format("delta").mode("overwrite").save(eventsOutputPath)
        data.write.format("delta").mode("overwrite").save("/tmp/delta-table")
        
        //Read
        val df = spark.read.format("delta").load("/tmp/delta-table")
        
        // 
        val streamingDf = spark.readStream.format("rate").load()
        ```
        
    - Schema
        
        You can use the **StructType** Scala method **toDDL** to have a DDL-formatted string created for you.
        
        ```scala
        usersDF.printSchema()  // print schema
        usersDF.schema   // print StructureType
        
        // Read schema to DDL
        val DDLSchema = spark.read.parquet("/../events/events.parquet").schema.toDDL
        
        val eventsDF = spark.read.schema(DDLSchema).json(eventsJsonPath)
        ```
        
        ```scala
        import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, LongType, StringType, StructType, StructField}
        
        val userDefinedSchema = StructType( Seq(
          StructField("user_id", StringType, true),
          StructField("email", StringType, true)
        ) )
        ```
        
    - DataFrame & Column
        
        A column is a logical construction that will be computed based on the data in a DataFrame using an expression
        
        ```scala
        col("device")
        $"device"
        eventsDF("device")
        ```
        
        `select()`
        
        ```scala
        import org.apache.spark.sql.functions.{col,lit}
        val df = eventsDF.select("user_id", "device")
        val df2 = eventsDF.select(col("user_id"),col("geo.city").alias("city"),lit("a lit"))
        ```
        
        New Column `selectExpr()`
        
        ```scala
        val appleDF = eventsDF.selectExpr("user_id", "device in ('iOS') as apple_user")
        val df= eventsDF.selectExpr("user_id", "3+1 as newColumn")
        ```
        
        Drop a column or columns `drop()`
        
        ```scala
        val anonymousDF = eventsDF.drop("user_id", "geo", "device")
        val noSalesDF = eventsDF.drop(col("ecommerce"))
        ```
        
        Add or replace columns  `withColumn()` `withColumnRenamed()`
        
        ```scala
        val df = eventsDF.withColumn("newColumnName", col("device").isin("iOS", "Android"))
        val df = eventsDF.withColumn("replaceColumnName", col("ecommerce.quantity").cast("int"))
        
        //Rename Column
        val df = eventsDF.withColumnRenamed("oldName", "newName")
        ```
        
        `dropDuplicates()`
        
        ```scala
        val df = eventsDF.dropDuplicates(Seq("device", "traffic_source"))
        val df2 = df.dropDuplicates()
        val df3 = df.distinct()
        ```
        
        `limit( )` new DataFrame by taking the **first n rows**. similar to top(10)
        
        ```scala
        val limitDF = eventsDF.limit(100)
        ```
        
        `sort()` Returns a new DataFrame sorted by. alias `oserBy()`
        
        ```scala
        val df = eventsDF.orderBy("touch_timestamp", "event_timestamp")
        val df = eventsDF.sort(col("touch_timestamp").desc, col("event_timestamp"))
        ```
        
    - Aggregation
        
         `groupBy( )`  to create a grouped data object This grouped data object is called `RelationalGroupedDataset` in **Scala** and `GroupedData` in **Python**
        
        ```scala
        df.groupBy("event_name")   //RelationalGroupedDataset 
        val eventCountsDF = df.groupBy("event_name").count()
        val df= df.groupBy("geo.state", "geo.city").sum("ecommerce.quantity")
        ```
        
        `agg` to apply built-in aggregate functions Built-in aggregate functions
        
        ```scala
        import org.apache.spark.sql.functions.{avg, approx_count_distinct, sum}
        val df= df.groupBy("geo.state").agg(sum("ecommerce.quantity").alias("purchases"))
        
        val stateAggregatesDF = df.groupBy("geo.state").agg(
          avg("ecommerce.quantity").alias("avg_quantity"),
          approx_count_distinct("user_id").alias("distinct_users"))
        ```
        
    - Datetime Functions
        
        `cast()` Casts column to a different data type, specified using string representation or DataType
        
        ```scala
        val df = df.withColumn("timestamp", (col("timestamp") / 1e6).cast("timestamp"))
        ```
        
        `date_format()`  Converts a date/timestamp/string to a **string** formatted from a given *date/timestamp/string*
        
        ```scala
        import org.apache.spark.sql.functions.date_format
        
        val df = timestampDF
        	.withColumn("date string", date_format(col("timestamp"), "MMMM dd, yyyy"))
          .withColumn("time string", date_format(col("timestamp"), "HH:mm:ss.SSSSSS"))
        ```
        
        `year()`, `month()`, `dayofweek()`, `minute()`, `second()` Extracts the year as an **integer** from a given *date/timestamp/string*.
        
        ```scala
        import org.apache.spark.sql.functions.{year, month, dayofweek, minute, second}
        
        val datetimeDF = timestampDF
        	.withColumn("year", year(col("timestamp")))
          .withColumn("month", month(col("timestamp")))
          .withColumn("dayofweek", dayofweek(col("timestamp")))
          .withColumn("minute", minute(col("timestamp")))
          .withColumn("second", second(col("timestamp")))
        ```
        
        `to_date()` Converts the column into DateType by casting rules to DateType from timestamp.
        
        ```scala
        import org.apache.spark.sql.functions.to_date
        val dateDF = timestampDF.withColumn("date", to_date(col("timestamp")))
        ```
        
        `date_add( col("columnName") , int)` Returns the date that is the given number of days after start
        
        ```scala
        import org.apache.spark.sql.functions.date_add
        val df = timestampDF.withColumn("plus_twoDays", date_add(col("timestamp"), 2))
        ```
        
    - Complex Types
        
        extract fields
        
        ```scala
        import org.apache.spark.sql.functions._
        
        val detailsDF = df.withColumn("items", explode(col("items")))
          .select("email", "items.item_name")      // one field
          .withColumn("details", split(col("item_name"), " ")) // array type
        
        ```
        
        `array_contains(col("column"), "word")` search a String into an array
        
        `element_at(col("column"), [index])` col("arrayColumn"), [index Integer]
        
        `filter( )`  returned a value if the condition is True
        
        ```scala
        val mattressDF = detailsDF.filter(array_contains(col("details"), "Mattress"))
          .withColumn("size", element_at(col("details"), 2))
        ```
        
        `df_a.unionByName(df_b)` union two DataFrames
        
        ```scala
        val unionDF = mattressDF.unionByName(pillowDF).drop("details")
        ```
        
        `agg( collect_set("col") )` returned an Array set
        
        ```scala
        val optionsDF = unionDF.groupBy("email")
          .agg(collect_set("size").alias("size options")
               )
        ```
        
- Laziness
    
    For large datasets, even a basic transformation will take millions of operations to execute. All you need to do is tell Spark what are the transformations you want to do on the dataset and Spark will maintain a series of transformations. When you ask for the results from Spark, it will then find out the best path and perform the required transformations and give you the result.
    
    ![https://cdn-images-1.medium.com/max/1024/1*2uwvLC1HsWpOsmRw4ZOp2w.png](https://cdn-images-1.medium.com/max/1024/1*2uwvLC1HsWpOsmRw4ZOp2w.png)
    
- [AQE]
    
    [How to Speed up SQL Queries with Adaptive Query Execution](https://databricks.com/blog/2020/05/29/adaptive-query-execution-speeding-up-spark-sql-at-runtime.html)
    
- Partitioning
    
    A partition in spark is an atomic chunk of data (logical division of data) stored on a node in the cluster. Partitions are basic units of parallelism in Apache Spark.
    
- Shuffling
    
    A shuffle occurs when data is rearranged between **partitions**. This is required when a transformation requires information from other partitions, such as summing all the values in a column. Spark will gather the required data from each partition and combine it into a new partition, likely on a different executor.
    
    During a shuffle, data is written to disk and transferred across the network, halting Spark’s ability to do processing in-memory and causing a performance bottleneck. Consequently we want to try to reduce the number of shuffles being done or reduce the amount of data being shuffled.
    
    ![Spark%20dd8609c5ca3041bb89b503bc314efe53/Untitled%201.png](Spark%20dd8609c5ca3041bb89b503bc314efe53/Untitled%201.png)
    
- Parallelism
    - to increase parallelism of spark processing is to increase the number of executors on the cluster
- Wide and Narrow Transformation
    
    Transformations is a kind of process that will transform your RDD data from one form to another in Spark. and when you apply this operation on an RDD, you will get a new RDD with transformed data (RDDs in Spark are immutable).
    
    - Narrow Transformations
        
        These types of transformations convert each input partition to only one output partition. When each partition at the parent RDD is used by at most one partition of the child RDD or when each partition from child produced or dependent on single parent RDD.
        
        - This kind of transformation is basically fast.
        - Does not require any data shuffling over the cluster network or no data movement.
        - Operation of `map()` and `filter()` belongs to this transformations.
        
        ![https://databricks.com/wp-content/uploads/2018/05/Narrow-Transformation.png](https://databricks.com/wp-content/uploads/2018/05/Narrow-Transformation.png)
        
    - Wide Transformations
        
        This type of transformation will have input partitions contributing to many output partitions. When each partition at the parent RDD is used by multiple partitions of the child RDD or when each partition from child produced or dependent on multiple parent RDD.
        
        - required to shuffle data around different nodes when creating new partitions
        - Functions such as `groupByKey()`, `aggregateByKey()`, `aggregate()`, `join()`, `repartition()` are some examples of wider transformations.
- Broadcast variable
- Broadcast join



[Databricks](Spark%20dd8609c5ca3041bb89b503bc314efe53/Databricks%2016b513dab9164637bb0c8f7f9f7d0013.md)