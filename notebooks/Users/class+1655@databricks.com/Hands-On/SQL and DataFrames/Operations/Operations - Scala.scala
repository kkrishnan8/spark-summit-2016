// Databricks notebook source exported at Wed, 17 Feb 2016 05:42:12 UTC
// MAGIC %md
// MAGIC # SQL and DataFrames Operations (Scala)
// MAGIC 
// MAGIC This notebook contains hands-on exercises that explore various Dataframe operations.

// COMMAND ----------

// MAGIC %md
// MAGIC ## The Dataset
// MAGIC 
// MAGIC Let's use the previous data set of (fake) people data again.

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC import java.sql.Timestamp
// MAGIC import java.text.SimpleDateFormat
// MAGIC 
// MAGIC val rdd = sc.textFile("dbfs:/mnt/training/dataframes/people.txt")
// MAGIC 
// MAGIC case class Person(firstName:  String, 
// MAGIC                   middleName: String,
// MAGIC                   lastName:   String, 
// MAGIC                   gender:     String, 
// MAGIC                   birthDate:  Timestamp,
// MAGIC                   salary:     Int,
// MAGIC                   ssn:        String)
// MAGIC 
// MAGIC val dateParser = new SimpleDateFormat("yyyy-MM-dd")
// MAGIC 
// MAGIC val df = rdd.map { line =>
// MAGIC   val cols = line.split(":")
// MAGIC   Person(cols(0), cols(1), cols(2), cols(3), new Timestamp(dateParser.parse(cols(4)).getTime), cols(5).toInt, cols(6))
// MAGIC }.toDF
// MAGIC 
// MAGIC df.printSchema()
// MAGIC df.show()

// COMMAND ----------

// MAGIC %md
// MAGIC **Question:** What could go wrong in the above code? How would you fix the problems?

// COMMAND ----------

// MAGIC %md Now, let's sample some of the data.

// COMMAND ----------

val sampledDF = df.sample(withReplacement = false, fraction = 0.02, seed = 1887348908234L)
display(sampledDF)

// COMMAND ----------

// MAGIC %md Next, let's run a couple SQL commands.

// COMMAND ----------

df.registerTempTable("people")

// COMMAND ----------

// MAGIC %sql SELECT * FROM people WHERE birthDate >= '1970-01-01' AND birthDate <= '1979-12-31' ORDER BY birthDate, salary

// COMMAND ----------

// MAGIC %sql SELECT concat(firstName, " ", lastName) AS name, gender, year(birthDate) AS birthYear, salary FROM people WHERE salary < 50000

// COMMAND ----------

// MAGIC %md
// MAGIC ## select and filter (and a couple more)
// MAGIC 
// MAGIC We've seen `printSchema()` and `show()`. Let's explore `select()` and `filter()`.

// COMMAND ----------

// MAGIC %md
// MAGIC Let's look at `select()`. It's like a SQL "SELECT" statement: It allows you to select the columns you want from a DataFrame.

// COMMAND ----------

df.select($"firstName", $"lastName", $"gender") // This returns another DataFrame

// COMMAND ----------

// MAGIC %md
// MAGIC **Remember**: Transformations are _lazy_. The `select()` method is a transformation.
// MAGIC 
// MAGIC All right. Let's look at result of a `select()` call.

// COMMAND ----------

df.select($"firstName", $"lastName", $"gender").show(10)

// COMMAND ----------

// MAGIC %md You can also create _derived_ columns. For example:

// COMMAND ----------

df.select($"firstName", $"lastName", $"gender", $"birthDate" > "1981-01-01").show()

// COMMAND ----------

// MAGIC %md You can do the same thing with SQL. In the following cell, what is _x_?

// COMMAND ----------

df.registerTempTable("people")
val x = sqlContext.sql("""SELECT firstName, lastName, gender, birthDate > "1981-01-01" FROM people""")

// COMMAND ----------

// MAGIC %md
// MAGIC Next, let's take a look at `filter()`, which can be used to filter data _out_ of a data set. `filter()` in the DataFrame API is equivalent to "WHERE" in a SQL command.
// MAGIC 
// MAGIC **Question**: What the does the following code actually do?

// COMMAND ----------

df.filter($"gender" === "M")

// COMMAND ----------

// MAGIC %md Note the use of a triple-equals (`===`) there. In Scala, that's required. You'll get a compiler error if you use `==`. (Try it.) If you like to switch between Python and Scala, be aware that you use double-equals (`==`) in Python and triple-equals in Scala.
// MAGIC 
// MAGIC `filter()`, like `select()`, is a transformation: It's _lazy_.
// MAGIC 
// MAGIC Let's try something a little more complicated. Let's combine two `filter()` operations with a `select()`, displaying the results.

// COMMAND ----------

val df2 = df.filter($"gender" === "M").filter($"salary" > 100000).select($"firstName", $"lastName", $"salary")
display(df2)

// COMMAND ----------

// MAGIC %md 
// MAGIC ## orderBy, groupBy and alias

// COMMAND ----------

// MAGIC %md What if we want to sort the output? That's easy enough in SQL:

// COMMAND ----------

// MAGIC %sql SELECT * FROM people WHERE birthDate >= '1970-01-01' AND birthDate <= '1979-12-31' ORDER BY birthDate, salary

// COMMAND ----------

// MAGIC %md Let's try that same query with the programmatic DataFrames API.

// COMMAND ----------

display( df.filter($"birthDate" >= "1970-01-01" && $"birthDate" <= "1979-12-31").orderBy(df("birthDate"), df("salary")) )

// COMMAND ----------

// MAGIC %md There are several things to note.
// MAGIC 
// MAGIC 1. This time, we _combined_ two filter expressions into one `filter()` call, instead of chaining two `filter()` calls.
// MAGIC 2. We did not have to convert the date literals ("1970-01-01" and "1979-12-31") into `java.sql.Timestamp` objects before using them in the comparisons.
// MAGIC 3. We used two different ways to specify the columns: `$("firstName")` and `df("firstName")`.
// MAGIC 
// MAGIC Let's try a `groupBy()` next. 
// MAGIC 
// MAGIC What is the return value of this statement?

// COMMAND ----------

df.groupBy($"salary")

// COMMAND ----------

// MAGIC %md Note that `groupBy()` returns something of type `GroupedData` (<http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.GroupedData>), instead of a `DataFrame`. There are other methods on `GroupedData` that will convert back to a DataFrame. A useful one is `count()`.
// MAGIC 
// MAGIC **WARNING**: Don't confuse `GroupedData.count()` with `DataFrame.count()`. `GroupedData.count()` is _not_ an action. `DataFrame.count()` _is_ an action.

// COMMAND ----------

val x = df.groupBy($"salary").count()  // What is x?

// COMMAND ----------

display(x)

// COMMAND ----------

// MAGIC %md Let's add a filter and, while we're at it, rename the `count` column.

// COMMAND ----------

display( df.groupBy($"salary").count().filter($"count" > 1).select($"salary", $"count".as("total")) )

// COMMAND ----------

// MAGIC %md
// MAGIC ### Renaming columns
// MAGIC 
// MAGIC Recall the following statement, from above:

// COMMAND ----------

df.select($"firstName", $"lastName", $"gender", $"birthDate" > "1981-01-01").show()

// COMMAND ----------

// MAGIC %md The derived column has an unhelpful name. But, we can rename it using `as` or `alias`:

// COMMAND ----------

df.select($"firstName", $"lastName", $"gender", ($"birthDate" > "1981-01-01").as("young")).show()

// COMMAND ----------

// MAGIC %md And, of course, we can do the same thing in SQL:

// COMMAND ----------

// MAGIC %sql SELECT firstName, lastName, gender, birthDate > "1981-01-01" AS young FROM people

// COMMAND ----------

// MAGIC %md Okay, let's go back to the slides for awhile.

// COMMAND ----------

