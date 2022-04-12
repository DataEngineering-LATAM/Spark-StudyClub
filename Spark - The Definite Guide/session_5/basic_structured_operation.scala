// Databricks notebook source
// MAGIC %md
// MAGIC # Chapter 5 - Basic Structured Operations
// MAGIC 
// MAGIC ##### Index
// MAGIC - Schemas
// MAGIC - Columns and Expressions:
// MAGIC   - `Columns`
// MAGIC   - `Expressions`
// MAGIC - Records and Rows: 
// MAGIC   - `Creating Rows`
// MAGIC - DataFrame Transformations: 
// MAGIC   - `Creating DataFrames`
// MAGIC   - `select and selectExpr`
// MAGIC   - `Converting to Spark Types (Literals)`
// MAGIC   - `Adding Columns`
// MAGIC   - `Renaming Columns`
// MAGIC   - `Reserved Characters and Keywords`
// MAGIC   - `Case Sensitivity`
// MAGIC   - `Removing Columns`
// MAGIC   - `Changing a Column’s Type (cast)`
// MAGIC   - `Filtering Rows`
// MAGIC   - `Getting Unique Rows`
// MAGIC   - `Random Samples`
// MAGIC   - `Random Splits`
// MAGIC   - `Concatenating and Appending Rows (Union)`
// MAGIC   - `Sorting Rows`
// MAGIC   - `Limit`
// MAGIC   - `Repartition and Coalesce`
// MAGIC   - `Collecting Rows to the Driver`
// MAGIC - Conclusion
// MAGIC 
// MAGIC - Documentación
// MAGIC <a href="https://www.oreilly.com/library/view/spark-the-definitive/9781491912201/" target="_blank">Spark: The Definitive Guide </a>

// COMMAND ----------

var df = spark.read.format("json").load("/FileStore/dataset/2015_summary.json")

// COMMAND ----------

// MAGIC %md
// MAGIC ### Schemas

// COMMAND ----------

df.printSchema()

// COMMAND ----------

df.schema

// COMMAND ----------

import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType}
import org.apache.spark.sql.types.Metadata

val myManualSchema = StructType(Array(
  StructField(
    "DEST_COUNTRY_NAME", StringType, true, Metadata.fromJson("{\"description\":\"Origin Country Name\"}")
  ),
  StructField(
    "ORIGIN_COUNTRY_NAME", StringType, true, Metadata.fromJson("{\"description\":\"Destine Country Name\"}")
  ),
  StructField(
    "count", LongType, false, Metadata.fromJson("{\"description\":\"number of flights\"}")
  )
))

var df = spark.read.format("json").schema(myManualSchema).load("/FileStore/dataset/2015_summary.json")

// COMMAND ----------

df.printSchema()
df.schema

// COMMAND ----------

df.schema.json

// COMMAND ----------

df.schema.foreach{s => println(s"${s.name}, ${s.metadata.toString}")}

// COMMAND ----------

display(spark.catalog.listDatabases)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Columns and Expressions

// COMMAND ----------

// MAGIC %md
// MAGIC  - ##### Columns

// COMMAND ----------

import org.apache.spark.sql.functions.{col, column}

col("someColumnName")
column("someColumnName")

// COMMAND ----------

$"myColumn"

// COMMAND ----------

'myColumn

// COMMAND ----------

df.col("count")

// COMMAND ----------

// MAGIC %md
// MAGIC  - ##### Expressions

// COMMAND ----------

// MAGIC %md
// MAGIC    - ###### Columns as expressions
// MAGIC 
// MAGIC expr("someCol") is equivalent to col("someCol")

// COMMAND ----------

// MAGIC %md
// MAGIC ![test](files/dataset/column_as_expression.JPG)

// COMMAND ----------

import org.apache.spark.sql.functions.expr
expr("(((someCol + 5) * 200) - 6) < otherCol")

// COMMAND ----------

// MAGIC %md
// MAGIC    - ###### Accessing a DataFrame’s columns

// COMMAND ----------

spark.read.format("json").load("/FileStore/dataset/2015_summary.json").columns

// COMMAND ----------

// MAGIC %md
// MAGIC ### Records and Rows

// COMMAND ----------

df.first()

// COMMAND ----------

// MAGIC %md
// MAGIC  - ##### Creating Rows

// COMMAND ----------

var myRow = Row("Hello",null,1,false)

// COMMAND ----------

println(myRow(0)) // type Any
println(myRow(0).asInstanceOf[String]) // String
println(myRow.isNullAt(1))// isNull
println(myRow.getInt(2)) // String
println(myRow.getBoolean(3)) // Bool

// COMMAND ----------

// MAGIC %md
// MAGIC ### DataFrame Transformations
// MAGIC ![test](files/dataset/dataframe_transformation.JPG)

// COMMAND ----------

// MAGIC %md
// MAGIC  - ##### Creating DataFrames

// COMMAND ----------

var df = spark.read.format("json").load("/FileStore/dataset/2015_summary.json")
df.createOrReplaceTempView("dfTable")

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dfTable;

// COMMAND ----------

import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType}

var myManualSchema = new StructType(Array(
new StructField("some", StringType, true),
new StructField("col", StringType, true),
new StructField("names", LongType, false)))

var myRows = Seq(Row("Hello", null, 1L))
var myRDD = spark.sparkContext.parallelize(myRows)
var myDf = spark.createDataFrame(myRDD, myManualSchema)

display(myDf)

// COMMAND ----------

df.createOrReplaceTempView("tmpView")

spark.sql("create table default.summar as select * from tmpView")

// COMMAND ----------

spark.catalog.listColumns("default","summar").show

// COMMAND ----------

spark.sql("show tables").show()

// COMMAND ----------

// MAGIC %sql
// MAGIC show tables;

// COMMAND ----------

// MAGIC %md
// MAGIC  - ##### select and selectExpr

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM dataFrameTable
// MAGIC SELECT columnName FROM dataFrameTable
// MAGIC SELECT columnName * 10, otherColumn, someOtherCol as c FROM dataFrameTable

// COMMAND ----------

df.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME").show(2)

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME FROM dfTable LIMIT 2

// COMMAND ----------

import org.apache.spark.sql.functions.{expr, col, column}

df.select(
  df.col("DEST_COUNTRY_NAME"),
  col("DEST_COUNTRY_NAME"),
  column("DEST_COUNTRY_NAME"),
  'DEST_COUNTRY_NAME,
  $"DEST_COUNTRY_NAME",
  expr("DEST_COUNTRY_NAME")
).show(2)

// COMMAND ----------

df.select(col("DEST_COUNTRY_NAME"), "DEST_COUNTRY_NAME")

// COMMAND ----------

df.select(expr("DEST_COUNTRY_NAME AS destination")).show(2)

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT DEST_COUNTRY_NAME as destination FROM dfTable LIMIT 2

// COMMAND ----------

df.select(expr("DEST_COUNTRY_NAME as destination").alias("DEST_COUNTRY_NAME")).show(2)

// COMMAND ----------

df.selectExpr("DEST_COUNTRY_NAME as newColumnName", "DEST_COUNTRY_NAME").show(2)

// COMMAND ----------

df.selectExpr(
"*", // include all original columns
"(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry")
.show(2)

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT *, (DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry
// MAGIC FROM dfTable
// MAGIC LIMIT 2

// COMMAND ----------

df.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))").show(2)

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT avg(count), count(distinct(DEST_COUNTRY_NAME)) FROM dfTable LIMIT 2

// COMMAND ----------

import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType}
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.functions.{expr, col, column, lit}
import spark.implicits._ // For implicit conversions

val mySchema = StructType(Array(
  StructField(
    "DEST_COUNTRY_NAME", StringType, true, Metadata.fromJson("{\"description\":\"Origin Country Name\"}")
  ),
  StructField(
    "ORIGIN_COUNTRY_NAME", StringType, true, Metadata.fromJson("{\"description\":\"Destine Country Name\"}")
  ),
  StructField(
    "count", LongType, false, Metadata.fromJson("{\"description\":\"number of flights\"}")
  )
))

var df = spark.read.format("json").schema(mySchema).load("/FileStore/dataset/2015_summary.json")
df.createOrReplaceTempView("dfTable")

// COMMAND ----------

// MAGIC %md
// MAGIC  - ##### Converting to Spark Types (Literals)

// COMMAND ----------

df.select(expr("*"), lit(1).as("One")).show(2)

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT *, 1 as One FROM dfTable LIMIT 2

// COMMAND ----------

// MAGIC %md
// MAGIC  - ##### Adding Columns

// COMMAND ----------

var df_new = df.withColumn("numberOne", lit(1))
df_new.show(2)

// COMMAND ----------

df_new = df_new.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME"))
df_new.show(2)

// COMMAND ----------

df_new.printSchema()

// COMMAND ----------

display(df_new_schema)

// COMMAND ----------

var cols_new2=List(("user2",lit("TIC")),("fecha2",expr("current_date()")))
var df_new_schema2 = cols_new2.foldLeft(df_new){ (tempdf, cols) => tempdf.withColumn(cols._1,cols._2) }
df_new_schema2.printSchema()

// COMMAND ----------

display(df_new_schema2)

// COMMAND ----------

// MAGIC %md
// MAGIC  - ##### Renaming Columns

// COMMAND ----------

df_new_schema2.withColumnRenamed("DEST_COUNTRY_NAME", "dest").show(2)

// COMMAND ----------

// MAGIC %md
// MAGIC  - ##### Reserved Characters and Keywords

// COMMAND ----------

display(df)

// COMMAND ----------

import org.apache.spark.sql.functions.expr

var dfWithLongColName = df.withColumn(
"This Long Column-Name",
expr("ORIGIN_COUNTRY_NAME"))

// COMMAND ----------

dfWithLongColName.show(2)

// COMMAND ----------

dfWithLongColName.selectExpr(
"`This Long Column-Name`",
"`This Long Column-Name` as `new col`")
.show(2)

// COMMAND ----------

dfWithLongColName.createOrReplaceTempView("dfTableLong")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT `This Long Column-Name`, `This Long Column-Name` as `new col`
// MAGIC FROM dfTableLong LIMIT 2

// COMMAND ----------

// MAGIC %md
// MAGIC  - ##### Case Sensitivity

// COMMAND ----------

spark.conf.get("spark.sql.caseSensitive");
//true-false
spark.conf.set("spark.sql.caseSensitive", "False");


// COMMAND ----------

// MAGIC %md
// MAGIC  - ##### Removing Columns

// COMMAND ----------

df.drop("ORIGIN_COUNTRY_NAME").columns

// COMMAND ----------

dfWithLongColName.drop("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").columns

// COMMAND ----------

// MAGIC %md
// MAGIC  - ##### Changing a Column’s Type (cast)

// COMMAND ----------

df.withColumn("count2", col("count").cast("long"))

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT *, cast(count as long) AS count2 FROM dfTable

// COMMAND ----------

// MAGIC %md
// MAGIC  - ##### Filtering Rows

// COMMAND ----------

df.filter(col("count") < 2).show(2)

// COMMAND ----------

df.where("count < 2").show(2)

// COMMAND ----------

df.where(col("count") < 2).where(col("ORIGIN_COUNTRY_NAME") =!= "Croatia")
.show(2)

// COMMAND ----------

// MAGIC %md
// MAGIC  - ##### Getting Unique Rows

// COMMAND ----------

df.select("ORIGIN_COUNTRY_NAME").count()

// COMMAND ----------

df.select("ORIGIN_COUNTRY_NAME").distinct().count()

// COMMAND ----------

// MAGIC %md
// MAGIC  - ##### Random Samples

// COMMAND ----------

var seed = 5
var withReplacement = false
var fraction = 0.8
df.sample(withReplacement, fraction, seed).count()

// COMMAND ----------

// MAGIC %md
// MAGIC  - ##### Random Splits

// COMMAND ----------

val dataFrames = df.randomSplit(Array(0.70, 0.30), seed)

// COMMAND ----------

dataFrames(0).count() > dataFrames(1).count()

// COMMAND ----------

display(dataFrames(0))

// COMMAND ----------

// MAGIC %md
// MAGIC  - ##### Concatenating and Appending Rows (Union)

// COMMAND ----------

import org.apache.spark.sql.Row

var schema = df.schema
var newRows = Seq(
Row("New Country", "Other Country", 5L),
Row("New Country 2", "Other Country 3", 1L)
)

var parallelizedRows = spark.sparkContext.parallelize(newRows)
var newDF = spark.createDataFrame(parallelizedRows, schema)

df.union(newDF)
.where("count = 1")
.where($"ORIGIN_COUNTRY_NAME" =!= "United States")
.show() 

// COMMAND ----------

// MAGIC %md
// MAGIC  - ##### Sorting Rows

// COMMAND ----------

df.sort("count").show(5)

// COMMAND ----------

df.orderBy("count", "DEST_COUNTRY_NAME").show(2)

// COMMAND ----------

import org.apache.spark.sql.functions.{desc, asc}

df.orderBy(expr("count desc")).show(2)

// COMMAND ----------

df.orderBy(desc("count"), asc("DEST_COUNTRY_NAME")).show(2)

df.createOrReplaceTempView("dfTable")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM dfTable ORDER BY count DESC, DEST_COUNTRY_NAME ASC LIMIT 2

// COMMAND ----------

var dfSort = spark.read.format("json").load("/FileStore/dataset/2015_summary.json")
.sortWithinPartitions("count")

// COMMAND ----------

display(dfSort)

// COMMAND ----------

// MAGIC %md
// MAGIC  - ##### Limit

// COMMAND ----------

df.limit(5).show()

// COMMAND ----------

df.orderBy(expr("count desc")).limit(6).show()

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM dfTable ORDER BY count desc LIMIT 6

// COMMAND ----------

// MAGIC %md
// MAGIC  - ##### Repartition and Coalesce 

// COMMAND ----------

// MAGIC %md
// MAGIC    * ###### Repartition

// COMMAND ----------

df.repartition(5)

// COMMAND ----------

df.repartition(col("DEST_COUNTRY_NAME"))

// COMMAND ----------

df.repartition(5, col("DEST_COUNTRY_NAME"))

// COMMAND ----------

// MAGIC %md
// MAGIC    * ######    Coalesce

// COMMAND ----------

df.repartition(5, col("DEST_COUNTRY_NAME")).coalesce(2)

// COMMAND ----------

// MAGIC %md
// MAGIC  - ##### Collecting Rows to the Driver 

// COMMAND ----------

var collectDF = df.limit(10)

// COMMAND ----------

display(collectDF)

// COMMAND ----------

var df1 = collectDF.take(5)
display(df1)

// COMMAND ----------

collectDF.show()

// COMMAND ----------

collectDF.show(5, false)

// COMMAND ----------

collectDF.collect()

// COMMAND ----------

collectDF.toLocalIterator()
