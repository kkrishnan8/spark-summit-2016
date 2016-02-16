// Databricks notebook source exported at Wed, 17 Feb 2016 05:41:03 UTC
// MAGIC %md
// MAGIC 
// MAGIC #![Wikipedia Logo](http://sameerf-dbc-labs.s3-website-us-west-2.amazonaws.com/data/wikipedia/images/w_logo_for_labs.png)
// MAGIC 
// MAGIC # Analyzing the Wikipedia PageCounts with RDDs
// MAGIC ### Time to complete: 20 minutes
// MAGIC 
// MAGIC #### Business questions:
// MAGIC 
// MAGIC * Question # 1) How many unique articles in English Wikipedia were requested in this hour?
// MAGIC * Question # 2) How many requests total did English Wikipedia get in this hour?
// MAGIC * Question # 3) How many requests total did each Wikipedia project get total during this hour?
// MAGIC * Question # 4) How many requests did the Apache Spark project recieve during this hour? Which language got the most requests?
// MAGIC * Question # 5) How many requests did the English Wiktionary project get during the captured hour?
// MAGIC * Question # 6) Which Apache project in English Wikipedia got the most hits during the capture hour?
// MAGIC * Question # 7) What were the top 10 pages viewed in English Wikipedia during the capture hour?
// MAGIC 
// MAGIC #### Technical Accomplishments:
// MAGIC 
// MAGIC * Learn how to use the following actions: count, take, takeSample, collect
// MAGIC * Learn the following transformations: filter, map, reduceByKey, sortBy
// MAGIC * Learn how to cache an RDD and view its number of partitions and total size in memory
// MAGIC * Learn how to send a closure function to a map transformation
// MAGIC * Learn how to define a case class to organize data in an RDD into objects
// MAGIC * Learn how to interpret a DAG visualization and understand the number of stages and tasks
// MAGIC * Learn why groupByKey should be avoided
// MAGIC 
// MAGIC 
// MAGIC 
// MAGIC Dataset: https://dumps.wikimedia.org/other/pagecounts-raw/

// COMMAND ----------

// MAGIC %md
// MAGIC ### Getting to know the Data
// MAGIC How large is the data? Let's use `%fs` to find out.

// COMMAND ----------

// MAGIC %fs ls /mnt/training-write/essentials/pagecounts/staging

// COMMAND ----------

// MAGIC %md 589722455  bytes means 589 MB.

// COMMAND ----------

// MAGIC %md Note that this file is from Nov 24, 2015 at 17:00 (5pm). It only captures 1 hour of page counts to all of Wikipedia languages and projects.

// COMMAND ----------

// MAGIC %md
// MAGIC ### RDDs
// MAGIC RDDs can be created by using the Spark Context object's `textFile()` method.

// COMMAND ----------

// In Databricks, the SparkContext is already created for you as the variable sc
sc

// COMMAND ----------

val pagecountsRDD = sc.textFile("dbfs:/mnt/training-write/essentials/pagecounts/staging/")

// COMMAND ----------

// MAGIC %md The `count` action counts how many items (lines) total are in the RDD:

// COMMAND ----------

val total = pagecountsRDD.count()

// COMMAND ----------

// MAGIC %md Let's make that a little easier for humans to read, with a little help from the Java standard library.

// COMMAND ----------

val numFormat = new java.text.DecimalFormat("###,###,###")
println(numFormat.format(total))

// COMMAND ----------

// MAGIC %md So there are about 7.9 million lines. Notice that the `count()` action took awhile to run: It had to scan the entire 96MB file remotely from S3.

// COMMAND ----------

// MAGIC %md You can use the take action to get the first K records (here K = 10):

// COMMAND ----------

pagecountsRDD.take(10)

// COMMAND ----------

// MAGIC %md The take command is much faster because it does not have read the entire file. 

// COMMAND ----------

// MAGIC %md Unfortunately this is not very readable because `take()` returns an, array and Scala simply prints the array with each element separated by a comma. We can make it prettier by traversing the array to print each record on its own line:

// COMMAND ----------

pagecountsRDD.take(10).foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC In the output above, the first column `aa` is the Wikimedia project name. The following abbreviations are used:
// MAGIC 
// MAGIC 
// MAGIC * `.b`: Wikibooks
// MAGIC * `.d`: Wiktionary
// MAGIC * `.m`: Wikimedia
// MAGIC * `.mw`: Wikipedia Mobile
// MAGIC * `.n`: WikiNews
// MAGIC * `.q`: Wikiquote
// MAGIC * `.s`: Wikisource
// MAGIC * `.v`: Wikiversity
// MAGIC * `.w`: Mediawiki
// MAGIC 
// MAGIC Projects without a period and a following character are Wikipedia projects.
// MAGIC 
// MAGIC The second column is the title of the page retrieved, the third column is the number of requests, and the fourth column is the size of the content returned.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Common RDD Transformantions and Actions
// MAGIC Next, we'll explore some common transformation and actions.

// COMMAND ----------

// MAGIC %md But first, let's cache our base RDD into memory. Again, this could take some time.

// COMMAND ----------

pagecountsRDD.setName("pagecountsRDD").repartition(4).cache.count

// COMMAND ----------

// MAGIC %md
// MAGIC You should now see the RDD in Spark UI's storage tab:
// MAGIC #![PagecountsRDD in Storage](http://i.imgur.com/ve6dMir.png)

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Question #1:
// MAGIC ** How many unique articles in English Wikipedia were requested in this hour?**

// COMMAND ----------

// MAGIC %md Let's filter out just the lines referring to English Wikipedia:

// COMMAND ----------

val enPagecountsRDD = pagecountsRDD.filter { _.startsWith("en ") }

// COMMAND ----------

// MAGIC %md Note that the above line is lazy and doesn't actually run the filter. We have to trigger the filter transformation to run by calling an action:

// COMMAND ----------

println(numFormat.format(enPagecountsRDD.count()))

// COMMAND ----------

// MAGIC %md Almost 2.4 million lines refer to the English Wikipedia project. So about half of the 5 million articles in English Wikipedia that are requested every hour. Let's take a look at 5 random lines:

// COMMAND ----------

enPagecountsRDD.takeSample(true, 5).foreach(println)

// COMMAND ----------

// MAGIC %md Some of pages are special pages that most Wikipedia users never see. For instance, something like ["Talk:Manhattan"](https://en.wikipedia.org/wiki/Talk:Manhattan) is a page for discussing changes to the [Manhattan](https://en.wikipedia.org/wiki/Manhattan) article. There are other prefixes, like "User:" and "Special:" that we might want to ignore. In fact, it's generally safe to ignore anything with a ":" in the article name, so let's do that.

// COMMAND ----------

val enCleanedPageCountsRDD = enPagecountsRDD.filter { line =>
  line.split("""\s+""") match {
    case Array(_, page, _, _) if page contains ":" => false
    case _ => true
  }
}
println(numFormat.format(enCleanedPageCountsRDD.count()))

// COMMAND ----------

// MAGIC %md
// MAGIC Okay, we've winnowed out about 400,000 rows. 

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Question #2:
// MAGIC ** How many requests total did English Wikipedia get in this hour?**

// COMMAND ----------

// MAGIC %md Let's define a function, `parse`, to parse out the 4 fields on each line. Then we'll run the parse function on each item in the RDD and create a new RDD named `enPagecountsParsedRDD`

// COMMAND ----------

// Define a function
def parse(line:String) = {
  val fields = line.split(' ') //Split the original line with 4 fields according to spaces
  (fields(0), fields(1), fields(2).toInt, fields(3).toLong) // return the 4 fields with their correct data types
}

// COMMAND ----------

// MAGIC %md ** Challenge 1:**  How would you use the parse function above in a `map` closure and assign the results to an RDD named *enPageCountsParsedRDD*?

// COMMAND ----------

// We'll put some code in here.

// COMMAND ----------

enPageCountsParsedRDD.takeSample(true, 3)

// COMMAND ----------

// MAGIC %md Using a combination of `map` and `take`, we can yank out just the requests field:

// COMMAND ----------

val requestCountsRDD = enPagecountsParsedRDD.map { case (_, _, requests, _) => requests }
requestCountsRDD.take(10)

// COMMAND ----------

// MAGIC %md ** Challenge 2:** Finally, let's sum all of the requests to English Wikipedia during the captured hour:

// COMMAND ----------

val sum = << FILL IN >>

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Question #3:
// MAGIC ** How many requests total did each Wikipedia project get total during this hour?**

// COMMAND ----------

// MAGIC %md Recall that our data file contains requests to all of the Wikimedia projects, including Wikibooks, Wiktionary, Wikinews, Wikiquote... and all of the 200+ languages.

// COMMAND ----------

// Use the parse function in a map closure
val allPagecountsParsedRDD = pagecountsRDD.map(parse)

// COMMAND ----------

allPagecountsParsedRDD.take(5).foreach(println)

// COMMAND ----------

// MAGIC %md Next, we'll create key/value pairs from the project prefix and the number of requests:

// COMMAND ----------

val requestsByProjectRDD = allPagecountsParsedRDD.map { case (project, _, requests, _) => (project, requests) }
requestsByProjectRDD.take(10)

// COMMAND ----------

// MAGIC %md Finally, we can use `reduceByKey()` to calculate the final answer:

// COMMAND ----------

val projectcountsRDD = requestsByProjectRDD.reduceByKey(_ + _)

// COMMAND ----------

// Sort by the value (number of requests) and pass in false to sort in descending order
projectcountsRDD.sortBy({ case (project, count) => count }, ascending=false).take(10).foreach(println)

// COMMAND ----------

// MAGIC %md We can see that the English Wikipedia Desktop and the English Wikipedia Mobile got the most hits this hour, followed by the German and Japanese Wikipedias.

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Question #4:
// MAGIC ** How many requests did the Apache Spark project recieve during this hour? Which language got the most requests?**

// COMMAND ----------

// MAGIC %md First we define a case class to organize our data in PageCount objects:

// COMMAND ----------

case class PageCount(project: String, title: String, requests: Long, size: Long) extends java.io.Serializable

// COMMAND ----------

val pagecountObjectsRDD = pagecountsRDD
  .map(_.split("""\s+"""))
  .filter(_.size == 4)
  .map(pc => PageCount(pc(0), pc(1), pc(2).toLong, pc(3).toLong))

// COMMAND ----------

// MAGIC %md Filter out just the items that mention "Apache_Spark" in the title:

// COMMAND ----------

pagecountObjectsRDD
  .filter(_.title.contains("Apache_Spark"))
  .count

// COMMAND ----------

// MAGIC %md ** Challenge 3:** Let's see if we can figure out which language edition of the Apache Spark page got the most hits.

// COMMAND ----------

// We'll plug some code in here.

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Question #5:
// MAGIC ** How many requests did the English Wiktionary project get during the captured hour?**

// COMMAND ----------

// MAGIC %md 
// MAGIC The [Wiktionary](https://en.wiktionary.org/wiki/Wiktionary:Main_Page) project is a free dictionary with 4 million+ entries from over 1,500 languages.

// COMMAND ----------

// MAGIC %md ** Challenge 4:** Can you figure this out? Start by figuring out the correct prefix that identifies the English Wikitionary project.

// COMMAND ----------

//Type in your answer here...
val enWiktionaryRDD = pagecountsRDD.filter { _.startsWith("en.d") }
println(numFormat.format(enWiktionaryRDD.count()))

// COMMAND ----------

// MAGIC %md The English Wikionary project got a total of 76,000 requests.

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Question #6:
// MAGIC ** Which Apache project in English Wikipedia got the most hits during the capture hour?**

// COMMAND ----------

// Here we reuse the PageCount case class we had defined earlier
val enPagecountObjectsRDD = enPagecountsRDD
  .map(_.split(' '))
  .filter(_.size == 4)
  .map(pc => PageCount(pc(0), pc(1), pc(2).toLong, pc(3).toLong))

// COMMAND ----------

enPagecountObjectsRDD
  .filter(_.title.contains("Apache_"))
  .collect()
  .foreach { x => println(s"${x.title}: ${x.requests}") }

// COMMAND ----------

// MAGIC %md Okay, our filter isn't perfect. Not surprisingly, there are other articles that start with "Apache" that aren't necessarily Apache Foundation software projects. However, since we're just exploring data here, we won't work too hard to filter those out.

// COMMAND ----------

enPagecountObjectsRDD
  .filter(_.title.contains("Apache_"))
  .map(x => (x.title, x.requests))
  .map(item => item.swap) // interchanges position of entries in each tuple
  .sortByKey(false, 1) // 1st arg configures ascending sort, 2nd arg configures one task
  .map(item => item.swap)
  .collect
  .foreach { case (title, requests) => println(s"$title: $requests") }

// COMMAND ----------

// MAGIC %md We can infer from the above results that Apache's Hadoop and Tomcat projects are the most popular, followed by Spark and the HTTP Server. Of course, these rankings might be different if we were to analyze page counts from a different period.

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Question #7:
// MAGIC ** What were the top 10 pages viewed in English Wikipedia during the capture hour?**

// COMMAND ----------

//Recall that we already have a RDD created that we can use for this analysis
enPageCountsParsedRDD

// COMMAND ----------

enPageCountsParsedRDD
  .takeSample(true, 5)
  .foreach(println)

// COMMAND ----------

enPageCountsParsedRDD
  .map { case (_, title, requests, _) => (title, requests) }
  .reduceByKey(_ + _)
  .sortBy(x => x._2, false)
  .take(10)
  .foreach(println)

// COMMAND ----------

// MAGIC %md It's not surprising that the main page is there. What about the other pages? Why are they getting so many hits?
// MAGIC 
// MAGIC * A little digging seems to show that the Anahí page correlates to pregnancy rumors about the Mexican pop star (proving that the U.S. isn't the only celebrity-obsessed culture).
// MAGIC * The film _Deadpool_ was just released, so it's not surprising to see a lot of hits for it.
// MAGIC * United States Supreme Court Justice Antonin Scalia just died, which explains the 7,785 hits on his page during this hour.

// COMMAND ----------

// MAGIC %md This concludes the RDD lab. 
// MAGIC 
// MAGIC But don't stop here ! You can learn more about the RDD API in Spark via the Spark docs:
// MAGIC https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.RDD

// COMMAND ----------

