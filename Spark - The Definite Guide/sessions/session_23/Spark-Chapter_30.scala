// Databricks notebook source
// MAGIC %md
// MAGIC # Chapter 30 - Graph Analytics
// MAGIC 
// MAGIC ##### Index
// MAGIC - Construir un grafo
// MAGIC - Consultar un grafo
// MAGIC   - `Subquery`
// MAGIC - Búsqueda de motivos
// MAGIC - Algoritmos de grafos
// MAGIC   - `PageRank`
// MAGIC   - `Métricas de grado de entrada y grado de salida`
// MAGIC   - `Búsqueda por orden de importancia`
// MAGIC   - `Componente conectado`
// MAGIC   - `Componentes fuertemente conectados`
// MAGIC - Conclusion
// MAGIC 
// MAGIC - Documentación
// MAGIC <a href="https://www.oreilly.com/library/view/spark-the-definitive/9781491912201/" target="_blank">Spark: The Definitive Guide </a>

// COMMAND ----------

// MAGIC %md
// MAGIC #### Lectura

// COMMAND ----------

val bikeStations = spark.read.option("header","true").csv("/FileStore/dataset/201508_station_data.csv")
val tripData = spark.read.option("header","true").csv("/FileStore/dataset/201508_trip_data.csv")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Construccion de un grafo

// COMMAND ----------

val stationVertices = bikeStations.withColumnRenamed("name", "id").distinct()
val tripEdges = tripData.withColumnRenamed("Start Station", "src").withColumnRenamed("End Station", "dst")

// COMMAND ----------

display(stationVertices)

// COMMAND ----------

display(tripEdges)

// COMMAND ----------

import org.graphframes.GraphFrame

// COMMAND ----------

val stationGraph = GraphFrame(stationVertices, tripEdges)
stationGraph.cache()

// COMMAND ----------

println(s"Total Number of Stations: ${stationGraph.vertices.count()}")
println(s"Total Number of Trips in Graph: ${stationGraph.edges.count()}")
println(s"Total Number of Trips in Original Data: ${tripData.count()}")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Consultas de grafo

// COMMAND ----------

import org.apache.spark.sql.functions.desc

// COMMAND ----------

display(
  stationGraph.edges.groupBy("src", "dst").count().orderBy(desc("count")).limit(10)
  )

// COMMAND ----------

display(
  stationGraph.edges
  .where("src = 'Townsend at 7th' OR dst = 'Townsend at 7th'")
  .groupBy("src", "dst").count()
  .orderBy(desc("count")).limit(10)
  )

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Subgrafo

// COMMAND ----------

val townAnd7thEdges = stationGraph.edges.where("src = 'Townsend at 7th' OR dst = 'Townsend at 7th'")
val subgraph = GraphFrame(stationGraph.vertices, townAnd7thEdges)

// COMMAND ----------

subgraph.vertices.count()

// COMMAND ----------

subgraph.edges.where("dst='Townsend at 7th'").count()

// COMMAND ----------

display(subgraph.inDegrees)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Búsqueda con motifs

// COMMAND ----------

// MAGIC %md
// MAGIC ![test](files/DE_Latam/img/3.png)

// COMMAND ----------

val motifs = stationGraph.find("(a)-[ab]->(b); (b)-[bc]->(c); (c)-[ca]->(a)")

// COMMAND ----------

// MAGIC %md 
// MAGIC * ##### ¿cuál es el viaje más corto que ha realizado la bicicleta desde la estación a, hasta la estación b, hasta la estación c y de vuelta a la estación a? 

// COMMAND ----------

import org.apache.spark.sql.functions.expr

// COMMAND ----------

display(
  motifs.selectExpr("*",
  "to_timestamp(ab.`Start Date`, 'MM/dd/yyyy HH:mm') as abStart",
  "to_timestamp(bc.`Start Date`, 'MM/dd/yyyy HH:mm') as bcStart",
  "to_timestamp(ca.`Start Date`, 'MM/dd/yyyy HH:mm') as caStart")
  .where("ca.`Bike #` = bc.`Bike #`").where("ab.`Bike #` = bc.`Bike #`")
  .where("a.id != b.id").where("b.id != c.id")
  .where("abStart < bcStart").where("bcStart < caStart")
  .orderBy(expr("cast(caStart as long) - cast(abStart as long)"))
  .selectExpr("a.id", "b.id", "c.id", "ab.`Start Date`", "ca.`End Date`")
  //.limit(1)
  )

// COMMAND ----------

// MAGIC %md
// MAGIC ## Graph Algorithms

// COMMAND ----------

// MAGIC %md 
// MAGIC #### PageRank

// COMMAND ----------

val ranks = stationGraph.pageRank.resetProbability(0.15).maxIter(10).run()
ranks.vertices.orderBy(desc("pagerank")).select("id", "pagerank").show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Métricas de grado de entrada y grado de salida

// COMMAND ----------

// MAGIC %md
// MAGIC ![test](files/DE_Latam/img/4.png)

// COMMAND ----------

val inDeg = stationGraph.inDegrees
inDeg.orderBy(desc("inDegree")).show(5, false)

// COMMAND ----------

val outDeg = stationGraph.outDegrees
outDeg.orderBy(desc("outDegree")).show(5, false)

// COMMAND ----------

val degreeRatio = inDeg.join(outDeg, Seq("id")).selectExpr("id", "double(inDegree)/double(outDegree) as degreeRatio")
degreeRatio.orderBy(desc("degreeRatio")).show(10, false)
degreeRatio.orderBy("degreeRatio").show(10, false)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Búsqueda por orden de importancia

// COMMAND ----------

stationGraph.bfs.fromExpr("id = 'Townsend at 7th'").toExpr("id = 'Spear at Folsom'").maxPathLength(2).run().show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Componente conectado

// COMMAND ----------

// MAGIC %md
// MAGIC ![test](files/DE_Latam/img/5.png)

// COMMAND ----------

spark.sparkContext.setCheckpointDir("/tmp/checkpoints")

// COMMAND ----------

val minGraph = GraphFrame(stationVertices, tripEdges.sample(false, 0.1))
val cc = minGraph.connectedComponents.run()

// COMMAND ----------

cc.where("component != 0").show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Componentes fuertemente conectados

// COMMAND ----------

val scc = minGraph.stronglyConnectedComponents.maxIter(3).run()

// COMMAND ----------


