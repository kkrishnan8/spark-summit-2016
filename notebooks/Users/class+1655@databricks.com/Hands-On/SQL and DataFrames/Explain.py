# Databricks notebook source exported at Tue, 16 Feb 2016 19:48:09 UTC
# MAGIC %md
# MAGIC # Explain
# MAGIC 
# MAGIC Let's take a brief look at the `explain()` function. First, let's load up a DataFrame in Scala and Python.

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val df = sqlContext.read.parquet("dbfs:/mnt/training/ssn/names.parquet")

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC df = sqlContext.read.parquet("dbfs:/mnt/training/ssn/names.parquet")
# MAGIC df.printSchema()

# COMMAND ----------

# MAGIC %md Next, we'll set up a couple queries, producing a second DataFrame.

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val df2 = df.filter($"total" > 1000).filter(($"year" >= 1880) && ($"year" <= 1900)).select($"firstName")

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC df2 = df.filter(df.total > 1000).filter((df.year >= 1880) & (df.year <= 1900)).select(df.firstName)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC df2.explain(true)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC df2.explain(True)