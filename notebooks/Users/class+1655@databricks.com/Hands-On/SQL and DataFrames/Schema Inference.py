# Databricks notebook source exported at Wed, 17 Feb 2016 05:42:25 UTC
# MAGIC %md # Playing Around with Schema Inference
# MAGIC 
# MAGIC (To be fair to the Python people, we'll use Python for this one.)

# COMMAND ----------

# MAGIC %md ## A File Without a Schema

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC rdd = sc.textFile("dbfs:/mnt/training/dataframes/people.txt")

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC rdd.take(10)

# COMMAND ----------

# MAGIC %md 
# MAGIC Okay, notice that we have a colon-delimited file here, and it appears to contain the following fields:
# MAGIC 
# MAGIC * First name
# MAGIC * Middle name
# MAGIC * Last name
# MAGIC * Gender
# MAGIC * Birth date
# MAGIC * Salary
# MAGIC * Social Security Number (Note: These are fake, in case you were thinking of getting a new credit card...)
# MAGIC 
# MAGIC It clearly has a human-readable schema, but it doesn't have a self-describing schema. How can we process this file with DataFrames?

# COMMAND ----------

# MAGIC %md ## Enter Schema Inference

# COMMAND ----------

# MAGIC %md ### Approach #1
# MAGIC 
# MAGIC Use a `namedtuple` in Python or a `case class` in Scala. We'll create a special class, `map` each row into an instance of that class, and then allow Spark to use that class to _infer_ the schema.

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from collections import namedtuple
# MAGIC from datetime import datetime
# MAGIC 
# MAGIC Person = namedtuple('Person', ('first_name', 'middle_name', 'last_name', 'gender', 'birth_date', 'salary', 'ssn'))
# MAGIC 
# MAGIC def make_person(line):
# MAGIC   cols = line.split(":")
# MAGIC   cols[4] = datetime.strptime(cols[4], '%Y-%M-%d')
# MAGIC   cols[5] = int(cols[5])
# MAGIC   return Person(*cols)
# MAGIC 
# MAGIC rdd2 = rdd.map(make_person)
# MAGIC 
# MAGIC # Now for the schema inference magic.
# MAGIC df = rdd2.toDF()
# MAGIC df.printSchema()
# MAGIC df.show()

# COMMAND ----------

# MAGIC %md Let's try the same thing in Scala. We'll make a separate base RDD, to keep things simple. We'll have to parse things slightly differently, and we'll use a `case class`.

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import java.sql.Timestamp
# MAGIC import java.text.SimpleDateFormat
# MAGIC 
# MAGIC val rdd = sc.textFile("dbfs:/mnt/training/dataframes/people.txt")
# MAGIC 
# MAGIC case class Person(firstName:  String, 
# MAGIC                   middleName: String,
# MAGIC                   lastName:   String, 
# MAGIC                   gender:     String, 
# MAGIC                   birthDate:  Timestamp,
# MAGIC                   salary:     Int,
# MAGIC                   ssn:        String)
# MAGIC 
# MAGIC val dateParser = new SimpleDateFormat("yyyy-MM-dd")
# MAGIC 
# MAGIC val df = rdd.map { line =>
# MAGIC   val cols = line.split(":")
# MAGIC   Person(
# MAGIC     cols(0), 
# MAGIC     cols(1), 
# MAGIC     cols(2), 
# MAGIC     cols(3), 
# MAGIC     new Timestamp(dateParser.parse(cols(4)).getTime), 
# MAGIC     cols(5).toInt, 
# MAGIC     cols(6)
# MAGIC   )
# MAGIC }.toDF
# MAGIC 
# MAGIC df.printSchema()
# MAGIC df.show()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Approach #2
# MAGIC 
# MAGIC We can also use a regular tuple (in both Python and Scala), but that only gives us _half_ the schema (the types). We'll have to specify the names manually.

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC def map_with_tuple(line):
# MAGIC   cols = line.split(":")
# MAGIC   cols[4] = datetime.strptime(cols[4], '%Y-%M-%d')
# MAGIC   cols[5] = int(cols[5])
# MAGIC   return tuple(cols)
# MAGIC   
# MAGIC df2 = rdd.map(map_with_tuple).toDF(("first_name", "middle_name", "last_name", "gender", "birth_date", "salary", "ssn"))
# MAGIC df2.printSchema()
# MAGIC df2.show()
# MAGIC   
# MAGIC   

# COMMAND ----------

# MAGIC %md And, in Scala.

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val df2 = rdd.map { line =>
# MAGIC   val cols = line.split(":")
# MAGIC   (cols(0), cols(1), cols(2), cols(3), new Timestamp(dateParser.parse(cols(4)).getTime), cols(5).toInt, cols(6))
# MAGIC }.toDF("firstName", "middleName", "lastName", "gender", "birthDate", "salary", "ssn")
# MAGIC 
# MAGIC df2.printSchema()
# MAGIC df2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Approach #3
# MAGIC 
# MAGIC The last schema inference approach we'll examine is the one all the others are based on. We'll use Scala, and for a good reason: In Scala, `case class` definitions are limited to 22 fields, and tuples are limited to 22 items. What do you do if you have to pull a schema-less file into a DataFrame, but it has more than 22 columns?
# MAGIC 
# MAGIC Well, you _could_ just use Python. But, you can also just drop down to the lower-level API. (And the lower-level API will also work in Python.)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.sql.types._
# MAGIC 
# MAGIC // This is the schema
# MAGIC val schema = StructType(
# MAGIC   List(
# MAGIC     StructField("firstName", StringType, nullable=true),
# MAGIC     StructField("middleName", StringType, nullable=true),
# MAGIC     StructField("lastName", StringType, nullable=true),
# MAGIC     StructField("gender", StringType, nullable=true),
# MAGIC     StructField("birthDate", TimestampType, nullable=true),
# MAGIC     StructField("salary", IntegerType, nullable=true),
# MAGIC     StructField("ssn", StringType, nullable=true)
# MAGIC   )
# MAGIC )
# MAGIC 
# MAGIC // Now, break each into rows.
# MAGIC val rowRDD = rdd.map { line =>
# MAGIC   val cols = line.split(":")
# MAGIC   val cols2 = Array(cols(0), cols(1), cols(2), cols(3), new Timestamp(dateParser.parse(cols(4)).getTime), cols(5).toInt, cols(6))
# MAGIC   Row(cols2: _*)
# MAGIC }
# MAGIC 
# MAGIC val df3 = sqlContext.createDataFrame(rowRDD, schema)
# MAGIC df3.printSchema()

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC display(df3)

# COMMAND ----------

# MAGIC %md Finally, no matter now Spark inferred the schema, you can always look at the underlying schema data structure:

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC pprint(df2.schema)

# COMMAND ----------

# MAGIC %scala 
# MAGIC 
# MAGIC df.schema