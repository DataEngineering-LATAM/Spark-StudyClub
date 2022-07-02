// Databricks notebook source
val df = spark.read.format("csv")
.option("header", "true")
.option("inferSchema", "true")
.load("dbfs:/FileStore/shared_uploads/bryanvivancosaldana@gmail.com/2010_12_01.csv")

df.printSchema()
df.createOrReplaceTempView("dfTable")

// COMMAND ----------

display(df)

// COMMAND ----------

// DBTITLE 1,Working with Booleans
// Working with Booleans
// true, false
import org.apache.spark.sql.functions.col

df.where(col("InvoiceNo").equalTo(536365))
.select("InvoiceNo", "Description")
.show(5, false)

df.where(col("InvoiceNo") === "536365" )
.select("InvoiceNo", "Description")
.show(5, false)

df.where("InvoiceNo = 536365")
.select("InvoiceNo", "Description")
.show(5, false)

// COMMAND ----------

//Working booleans
//and, or
val priceFilter = col("UnitPrice") > 600
val descripFilter = col("Description").contains("POSTAGE")

df
.where(col("StockCode").isin("DOT"))
.where(priceFilter.or(descripFilter))
.show(5, false)

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM dfTable WHERE StockCode in ("DOT") AND(UnitPrice > 600 OR instr(Description, "POSTAGE") >= 1)

// COMMAND ----------

val DOTCodeFilter = col("StockCode") === "DOT"
val priceFilter = col("UnitPrice") > 600
val descripFilter = col("Description").contains("POSTAGE")

df.withColumn("isExpensive", DOTCodeFilter.and(priceFilter.or(descripFilter)))
.where("isExpensive") //-> filter boolean is true 
.select("unitPrice", "isExpensive").show(5)

// COMMAND ----------

// in Scala
//same performance
import org.apache.spark.sql.functions.{expr, not, col}
df.withColumn("isExpensive", not(col("UnitPrice").leq(250)))
.filter("isExpensive")
.select("Description", "UnitPrice").show(5)

df.withColumn("isExpensive", expr("NOT UnitPrice <= 250"))
.filter("isExpensive")
.select("Description", "UnitPrice").show(5)

// COMMAND ----------

// DBTITLE 1,Working with Numbers
//Working with Numbers
//eq = (the current quantity * the unit price) + 5

import org.apache.spark.sql.functions.{expr, pow}
val fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"), 2) + 5
df.select(expr("CustomerId"), fabricatedQuantity.alias("realQuantity")).show(2)


// COMMAND ----------

// in Scala
df.selectExpr(
"CustomerId",
"(POWER((Quantity * UnitPrice), 2.0) + 5) as realQuantity").show(2)

// COMMAND ----------

import org.apache.spark.sql.functions.{round, bround}
df.select(round(col("UnitPrice"), 1).alias("rounded"), col("UnitPrice")).show(5)

import org.apache.spark.sql.functions.lit
df.select(round(lit("2.5")), bround(lit("2.5"))).show(2)

// COMMAND ----------

// in Scala
import org.apache.spark.sql.functions.{corr}
df.stat.corr("Quantity", "UnitPrice")
df.select(corr("Quantity", "UnitPrice")).show()

// COMMAND ----------

// in Scala
df.describe().show()

// COMMAND ----------

val colName = "UnitPrice"
val quantileProbs = Array(0.5)
val relError = 0.05

df.stat.approxQuantile("UnitPrice", quantileProbs, relError) // 2.51

// COMMAND ----------

// DBTITLE 1,Working with Strings
// in Scala
import org.apache.spark.sql.functions.{initcap}
df.select(initcap(col("Description")), col("Description")).show(2, false)

// COMMAND ----------

import org.apache.spark.sql.functions.{lower, upper}
df.select(col("Description"),
lower(col("Description")),
upper(lower(col("Description")))).show(2)

// COMMAND ----------

// in Scala
import org.apache.spark.sql.functions.{lit, ltrim, rtrim, rpad, lpad, trim}
df.select(
ltrim(lit(" HELLO ")).as("ltrim"),
rtrim(lit(" HELLO ")).as("rtrim"),
trim(lit(" HELLO ")).as("trim"),
lpad(lit("HELLO"), 3, " ").as("lp"),
rpad(lit("HELLO"), 10, " ").as("rp")).show(2)

// COMMAND ----------

// DBTITLE 1,Regex 
// regexp_replace
import org.apache.spark.sql.functions.regexp_replace
val simpleColors = Seq("black", "white", "red", "green", "blue")
val regexString = simpleColors.map(_.toUpperCase).mkString("|")
// the | signifies `OR` in regular expression syntax
df.select(
regexp_replace(col("Description"), regexString, "COLOR").alias("color_clean"),
col("Description")).show(2, false)

regexString

// COMMAND ----------

// regexp_extract
import org.apache.spark.sql.functions.regexp_extract
val regexString1 = simpleColors.map(_.toUpperCase).mkString("(", "|", ")")
// the | signifies OR in regular expression syntax
df.select(
regexp_extract(col("Description"), regexString1, 1).alias("color_clean"),
col("Description")).show(2, false)

regexString1

// COMMAND ----------

val containsBlack = col("Description").contains("BLACK")
val containsWhite = col("DESCRIPTION").contains("WHITE")
df.withColumn("hasSimpleColor", containsBlack.or(containsWhite))
.where("hasSimpleColor")
.select("Description").show(3, false)

// COMMAND ----------

//search especific value
df.select(col("StockCode").rlike("^([A-Za-z2-9])*$").as("matching Regex"), col("StockCode")).show(100, false)

// COMMAND ----------

// DBTITLE 1,Working with Dates and Timestamps
//Working with Dates and Timestamps
import org.apache.spark.sql.functions.{current_date, current_timestamp}
val dateDF = spark.range(10)
.withColumn("today", current_date())
.withColumn("now", current_timestamp())

dateDF.createOrReplaceTempView("dateTable")
dateDF.printSchema()

// COMMAND ----------

dateDF.show(10, false)

// COMMAND ----------

import org.apache.spark.sql.functions.{date_add, date_sub}
dateDF
.select(
  date_sub(col("today"), 5),
  date_add(col("today"), 5))
.show(1)

// COMMAND ----------

import org.apache.spark.sql.functions.{datediff, months_between, to_date}
import org.apache.spark.sql.functions.lit

dateDF
.withColumn("week_ago", date_sub(col("today"), 7))
.select(
  datediff(col("week_ago"), col("today")),
  col("week_ago"),
  col("today"))
.show(1)

dateDF.select(
  to_date(lit("2016-01-01")).alias("start"),
  to_date(lit("2017-05-22")).alias("end"))
.select(months_between(col("start"), col("end")))
.show(1)


// COMMAND ----------

import org.apache.spark.sql.functions.{to_date, lit}

spark.range(1).withColumn("date", lit("2017-01-01"))
.select(to_date(col("date")))
.show(1)

// COMMAND ----------

//issues by differents format
dateDF
.select(
  to_date(lit("2016-20-12")),
  to_date(lit("2017-12-11")))
.show(1)

// COMMAND ----------

//  fix: to_date 
import org.apache.spark.sql.functions.to_date

val dateFormat = "yyyy-dd-MM"

val cleanDateDF = spark.range(1).select(
  to_date(lit("2017-12-11"), dateFormat).alias("date"),
  to_date(lit("2017-20-12"), dateFormat).alias("date2"),
  to_date(lit("2021-31-12")).alias("date3"))
cleanDateDF.createOrReplaceTempView("dateTable2")

cleanDateDF.show(false)

// COMMAND ----------

// comparison is true
cleanDateDF.filter(col("date2") > lit("2017-12-12")).show()
// comparison is false
cleanDateDF.filter(col("date2") > lit("2017-12-30")).show()

// COMMAND ----------

// DBTITLE 1,Working with Nulls
import org.apache.spark.sql.types._

val data = Seq(
  Row(90, "computer science"),
  Row(100, "AI"),
  Row(70, "Stadistics"),
  //Row(10, null),
  Row(null, "Data Engineer"),
  Row(null, null),
)

val schema = StructType(
  List(
    StructField("power_code", IntegerType, true),
    StructField("career", StringType, true)
  )
)

val df1 = spark.createDataFrame(
  spark.sparkContext.parallelize(data),
  schema
)

df1.printSchema()
df1.show(false)

// COMMAND ----------

//drop
val dfNull = df1.na.drop()
val dfNull1 = df1.na.drop("any")
val dfNull2 = df1.na.drop("all")

val dfNull3 = df1.na.drop("all", Seq("power_code"))

// COMMAND ----------

display(dfNull3)

// COMMAND ----------

//fill
val dfFill = df1.na.fill("All Null values become this string")
val dfFill1 = df1.na.fill(999, Seq("power_code"))
display(dfFill1)

// COMMAND ----------

val count = df1.count()

// COMMAND ----------

// in Scala
df.na.fill(5, Seq("StockCode", "InvoiceNo"))

// COMMAND ----------

val dfNuevo = df.na.fill("All Null values become this string")
display(dfNuevo)

// COMMAND ----------

// DBTITLE 1,Working with Complex Types
val dfComplex = df.selectExpr("(Description, InvoiceNo) as complex", "*")
display(dfComplex)

// COMMAND ----------

import org.apache.spark.sql.functions.struct
val complexDF = df.select(struct("Description", "InvoiceNo").alias("complex"))
complexDF.createOrReplaceTempView("complexDF")

display(complexDF)

// COMMAND ----------

//get field
val dfComplex1 = complexDF.select("complex.InvoiceNo")
val dfComplex2 = complexDF.select(col("complex").getField("Description"))

display(dfComplex2)

// COMMAND ----------

// Arrays
import org.apache.spark.sql.functions.split
display(df.select(split(col("Description"), " ")))

// COMMAND ----------

// in Scala
df.select(split(col("Description"), " ").alias("array_col"))
.selectExpr("array_col[0]").show(2)

// COMMAND ----------

// array_contains
import org.apache.spark.sql.functions.array_contains
df.select(array_contains(split(col("Description"), " "), "WHITE")).show(2)

// COMMAND ----------

//explode

import org.apache.spark.sql.functions.{split, explode}
val dfExplode = df.withColumn("splitted", split(col("Description"), " "))
.withColumn("exploded", explode(col("splitted")))
.select("Description", "InvoiceNo", "exploded").limit(2)

display(dfExplode)

// COMMAND ----------

// maps key value
import org.apache.spark.sql.functions.map
val dfMap = df.select(map(col("Description"), col("InvoiceNo")).alias("complex_map")).limit(2)
display(dfMap)

// COMMAND ----------

val dfMap1 = df.select(map(col("Description"), col("InvoiceNo")).alias("complex_map"))
.selectExpr("complex_map['WHITE METAL LANTERN']").limit(2)

display(dfMap1)
