// Databricks notebook source exported at Tue, 16 Feb 2016 19:40:29 UTC
// MAGIC %md
// MAGIC # RDD Lab (Scala)
// MAGIC 
// MAGIC In this lab, we'll explore some of the RDD concepts we've discussed. We'll be using a data set consisting of reported crimes in Washington D.C. in 2013. This data comes from the [District of Columbia's Open Data Catalog](http://data.octo.dc.gov/). We'll use this data to explore some RDD transitions and actions.
// MAGIC 
// MAGIC ## Exercises and Solutions
// MAGIC 
// MAGIC This notebook contains a number of exercises. If, at any point, you're struggling with the solution to an exercise, feel free to look in the **Solutions** notebook (in the same folder as this lab).
// MAGIC 
// MAGIC ## Let's get started.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Load the data
// MAGIC 
// MAGIC The first step is to load the data. Run the following cell to create an RDD containing the data.

// COMMAND ----------

val baseRDD = sc.textFile("dbfs:/mnt/training/wash_dc_crime_incidents_2013.csv")

// COMMAND ----------

// MAGIC %md **Question**: Does the RDD _actually_ contain the data right now?
// MAGIC **Answer**: No

// COMMAND ----------

// MAGIC %md
// MAGIC ## Explore the data
// MAGIC 
// MAGIC Let's take a look at some of the data.

// COMMAND ----------

baseRDD.take(10).foreach(println)

// COMMAND ----------

// MAGIC %md Okay, there's a header. We'll need to remove that. But, since the file will be split into partitions, we can't just drop the first item. Let's figure out another way to do it.

// COMMAND ----------

val noHeaderRDD = baseRDD.filter { line => ! (line contains "REPORTDATETIME") }

// COMMAND ----------

noHeaderRDD.take(10).foreach(println)

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Exercise 1
// MAGIC 
// MAGIC Let's make things a little easier to handle, by converting the `noHeaderRDD` to an RDD containing Scala objects.
// MAGIC 
// MAGIC **TO DO**
// MAGIC 
// MAGIC * Split each line into its individual cells.
// MAGIC * Map the RDD into another RDD of appropriate `CrimeData` objects.

// COMMAND ----------

// TAKE NOTE: We are deliberately only keeping the first five fields of
// each line, since that's all we're using in this lab. There's no sense
// in dragging around more data than we need.
case class CrimeData(ccn: String, 
                     reportTime: String,
                     shift: String,
                     offense: String,
                     method: String)
                     
val dataRDD = noHeaderRDD.map { line =>  {
    val cols = line.split("\\,")
    CrimeData(cols(0), cols(1), cols(2), cols(3), cols(4))
  }
}
dataRDD.take(10).foreach(println)

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Exercise 2
// MAGIC 
// MAGIC Next, group the data by type of crime (the "OFFENSE" column).

// COMMAND ----------

val groupedByOffenseRDD = dataRDD.groupBy { data => data.offense }

// What does this return? You'll need to know for the next step.
groupedByOffenseRDD.take(10).foreach(println)

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Exercise 3
// MAGIC Next, create an RDD that counts the number of each offense. How many murders were there in 2013? How many assaults with a dangerous weapon?

// COMMAND ----------

val offenseCounts = groupedByOffenseRDD.map( f => (f._1, f._2.size)).collect
for ((offense, count) <- offenseCounts) {
  println(s"$offense -> $count")
}

// COMMAND ----------

// MAGIC %md ### Question
// MAGIC 
// MAGIC Run the following cell. Can you explain what happened? Is `collectAsMap()` a _transformation_ or an _action_?

// COMMAND ----------

groupedByOffenseRDD.collectAsMap()

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Exercise 4
// MAGIC 
// MAGIC How many partitions does the base RDD have? What about the `groupedByOffenseRDD` RDD? How can you find out?
// MAGIC 
// MAGIC **Hint**: Check the [API documentation](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.RDD).

// COMMAND ----------

println(baseRDD.<FILL-IN>)
println(groupedByOffenseRDD.<FILL-IN>)

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Exercise 5
// MAGIC 
// MAGIC Since we're continually playing around with this data, we might as well cache it, to speed things up.
// MAGIC 
// MAGIC **Question**: Which RDD should you cache? 
// MAGIC 
// MAGIC 1. `baseRDD`
// MAGIC 2. `noHeaderRDD`
// MAGIC 3. `dataRDD`
// MAGIC 4. None of them, because they're all still too big.
// MAGIC 5. It doesn't really matter.

// COMMAND ----------

<FILL-IN>.cache()

// COMMAND ----------

// MAGIC %md ### Exercise 6
// MAGIC 
// MAGIC Display the number of homicides by weapon (method).

// COMMAND ----------

val resultRDD1 = dataRDD.<FILL-IN>
println(resultRDD1.collect())

// BONUS: Make the output look better, using a for loop or a foreach.

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Exercise 7
// MAGIC 
// MAGIC During which police shift did the most crimes occur in 2013?

// COMMAND ----------

// Hint: Start with the dataRDD
println(dataRDD.<FILL-IN>)