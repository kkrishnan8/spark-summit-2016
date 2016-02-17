# Databricks notebook source exported at Wed, 17 Feb 2016 05:40:08 UTC
# MAGIC %md
# MAGIC # RDD Lab (Python)
# MAGIC 
# MAGIC In this lab, we'll explore some of the RDD concepts we've discussed. We'll be using a data set consisting of reported crimes in Washington D.C. in 2013. This data comes from the [District of Columbia's Open Data Catalog](http://data.octo.dc.gov/). We'll use this data to explore some RDD transitions and actions.
# MAGIC 
# MAGIC ## Exercises and Solutions
# MAGIC 
# MAGIC This notebook contains a number of exercises. If, at any point, you're struggling with the solution to an exercise, feel free to look in the **Solutions** notebook (in the same folder as this lab).
# MAGIC 
# MAGIC ## Let's get started.
# MAGIC 
# MAGIC First, let's import a couple things we'll need.

# COMMAND ----------

from collections import namedtuple
from pprint import pprint

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load the data
# MAGIC 
# MAGIC The next step is to load the data. Run the following cell to create an RDD containing the data.

# COMMAND ----------

base_rdd = sc.textFile("dbfs:/mnt/training/wash_dc_crime_incidents_2013.csv")

# COMMAND ----------

# MAGIC %md **Question**: Does the RDD _actually_ contain the data right now?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explore the data
# MAGIC 
# MAGIC Let's take a look at some of the data.

# COMMAND ----------

base_rdd.take(10)

# COMMAND ----------

# MAGIC %md Okay, there's a header. We'll need to remove that. But, since the file will be split into partitions, we can't just drop the first item. Let's figure out another way to do it.

# COMMAND ----------

no_header_rdd = base_rdd.filter(lambda line: 'REPORTDATETIME' not in line)

# COMMAND ----------

no_header_rdd.take(10)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Exercise 1
# MAGIC 
# MAGIC Let's make things a little easier to handle, by converting the `no_header_rdd` to an RDD containing Python objects.
# MAGIC 
# MAGIC **TO DO**
# MAGIC 
# MAGIC * Split each line into its individual cells.
# MAGIC * Map the RDD into another RDD of appropriate `namedtuple` objects.

# COMMAND ----------

# Replace the <FILL-IN> sections with appropriate code.

# TAKE NOTE: We are deliberately only keeping the first five fields of
# each line, since that's all we're using in this lab. There's no sense
# in dragging around more data than we need.
CrimeData = namedtuple('CrimeData', ['ccn', 'report_time', 'shift', 'offense', 'method'])

def map_line(line):
  columns = <FILL-IN>
  return <FILL-IN>

data_rdd = no_header_rdd.map(map_line)
print data_rdd.take(10)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Exercise 2
# MAGIC 
# MAGIC Next, group the data by type of crime (the "OFFENSE" column).

# COMMAND ----------

grouped_by_offense_rdd = data_rdd.groupBy(lambda data: <FILL-IN>)

# What does this return? You'll need to know for the next step.
print grouped_by_offense_rdd.take(10)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Exercise 3
# MAGIC Next, create an RDD that counts the number of each offense. How many murders were there in 2013? How many assaults with a dangerous weapon?

# COMMAND ----------

offense_counts = <FILL-IN>
for offense, count in <FILL-IN>:
  print "{0:30s} {1:d}".format(offense, count)

# COMMAND ----------

# MAGIC %md ### Question
# MAGIC 
# MAGIC Run the following cell. Can you explain what happened? Is `collectAsMap()` a _transformation_ or an _action_?

# COMMAND ----------

grouped_by_offense_rdd.collectAsMap()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Exercise 4
# MAGIC 
# MAGIC How many partitions does the base RDD have? What about the `grouped_by_offense` RDD? How can you find out?
# MAGIC 
# MAGIC **Hint**: Check the [API documentation](http://spark.apache.org/docs/latest/api/python/pyspark.html#pyspark.RDD).

# COMMAND ----------

print base_rdd.<FILL-IN>
print grouped_by_offense_rdd.<FILL-IN>

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Exercise 5
# MAGIC 
# MAGIC Since we're continually playing around with this data, we might as well cache it, to speed things up.
# MAGIC 
# MAGIC **Question**: Which RDD should you cache? 
# MAGIC 
# MAGIC 1. `base_rdd`
# MAGIC 2. `no_header_rdd`
# MAGIC 3. `data_rdd`
# MAGIC 4. None of them, because they're all still too big.
# MAGIC 5. It doesn't really matter.

# COMMAND ----------

<FILL-IN>.cache()

# COMMAND ----------

# MAGIC %md ### Exercise 6
# MAGIC 
# MAGIC Display the number of homicides by weapon (method).

# COMMAND ----------

result_rdd1 = data_rdd.<FILL-IN>
print result_rdd1.collect()

# BONUS: Make the output look better, using a for loop or a list comprehension.

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Exercise 7
# MAGIC 
# MAGIC During which police shift did the most crimes occur in 2013?

# COMMAND ----------

# Hint: Start with the data_rdd
print data_rdd.<FILL-IN>