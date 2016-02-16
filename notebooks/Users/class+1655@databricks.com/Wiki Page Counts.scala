// Databricks notebook source exported at Tue, 16 Feb 2016 18:33:06 UTC
sc

// COMMAND ----------

val pagecountsRDD = sc.textFile("dbfs:/mnt/training-write/essentials/pagecounts/staging/")

// COMMAND ----------

val total = pagecountsRDD.count()

// COMMAND ----------

pagecountsRDD.take(10)

// COMMAND ----------

// MAGIC %md ###You can document using markdown

// COMMAND ----------

val enPagecountsRDD = pagecountsRDD.filter{ line => line.startsWith("en") }

// COMMAND ----------

println(enPagecountsRDD.count)

// COMMAND ----------

enPagecountsRDD.takeSample(true, 5).foreach(println)

// COMMAND ----------

val enCleanedPagecountsRDD = enPagecountsRDD.filter{line => line.contains("Java")}

// COMMAND ----------

println(enCleanedPagecountsRDD.count)

// COMMAND ----------

val enPageCountsParsedRDD = enCleanedPagecountsRDD.map{ line => {
    val cols = line.split(" ")
    (cols(0), cols(1), cols(2).toInt, cols(3).toInt)
  }
}

// COMMAND ----------

enPageCountsParsedRDD.take(5).foreach(println)

// COMMAND ----------

val enRequestByProjectRDD = enPageCountsParsedRDD.map { case(project, _, requests, _) => (project, requests) }

// COMMAND ----------

enRequestByProjectRDD.take(5).foreach(println)

// COMMAND ----------

// MAGIC %md ###AVOID using groupByKey() 

// COMMAND ----------

val keyCountRDD = enRequestByProjectRDD.reduceByKey(_ + _)

// COMMAND ----------

keyCountRDD.collect.foreach(println)