// Databricks notebook source exported at Wed, 17 Feb 2016 05:42:51 UTC
// MAGIC %md
// MAGIC # Spark Streaming and Wikipedia ![Wikipedia Logo](http://sameerf-dbc-labs.s3-website-us-west-2.amazonaws.com/data/wikipedia/images/w_logo_for_labs.png)
// MAGIC 
// MAGIC ### Time to complete: 20-30 minutes
// MAGIC 
// MAGIC #### Business Questions:
// MAGIC 
// MAGIC * Question 1: Where are recent anonymous English Wikipedia editors located in the world?
// MAGIC * Question 2: Are edits being made to the English Wikipedia from non-English speaking countries? If so, where?
// MAGIC 
// MAGIC #### Technical Accomplishments:
// MAGIC 
// MAGIC * Increased understanding of the Spark Streaming UI.
// MAGIC * How to visualize data in a Databricks notebook.

// COMMAND ----------

// MAGIC %md 
// MAGIC ## Preparation
// MAGIC 
// MAGIC In this lab, we're going to analyze some streaming Wikipedia edit data.
// MAGIC 
// MAGIC First, let's get some imports out of the way.

// COMMAND ----------

import com.databricks.training.helpers.Enrichments.EnrichedArray
import com.databricks.training.helpers.TimeConverters._
import com.databricks.training.helpers.TimeHelpers._
import com.databricks.training.helpers.InetHelpers._
import com.databricks.training.helpers.uniqueIDForUser

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.streaming._
import scala.util.Random

// COMMAND ----------

// MAGIC %md Now, some configuration constants.

// COMMAND ----------

val ID                   = uniqueIDForUser() // library routine
val StreamingServerHost  = "54.68.10.240"
val StreamingServerPort  = 9002
val TableName            = s"""Edits_${ID.replace("-", "_")}"""
val DFTableAlias         = s"""TempEdits_${ID.replace("-", "_")}"""
val BatchInterval        = Seconds(15)

// COMMAND ----------

// MAGIC %md We're going to be buffering our data in a Hive table, so let's make sure a fresh one exists.

// COMMAND ----------

sqlContext.sql(s"DROP TABLE IF EXISTS $TableName")
sqlContext.sql(s"CREATE TABLE $TableName (user STRING, anonymous BOOLEAN, pageurl STRING, page STRING, timestamp TIMESTAMP, robot BOOLEAN)")

// COMMAND ----------

// MAGIC %md
// MAGIC ## The Streaming Logic
// MAGIC 
// MAGIC The following couple of cells contain our Spark Streaming logic.
// MAGIC 
// MAGIC The Streaming code:
// MAGIC 
// MAGIC * Receives each Streaming-produced RDD, which contains individual change records as JSON objects (strings)
// MAGIC * Converts the RDD of string JSON records into a DataFrame
// MAGIC * Selects only the columns we really care to process
// MAGIC * Saves the changes to a Hive table
// MAGIC 
// MAGIC ### The fields we're keeping
// MAGIC 
// MAGIC Here are the fields we're keeping from the incoming data:
// MAGIC 
// MAGIC * `anonymous` (`BOOLEAN`): Whether or not the change was made by an anonymous user.
// MAGIC * `pageUrl` (`STRING`): The URL of the page that was edited.
// MAGIC * `page`: (`STRING`): The printable name of the page that was edited
// MAGIC * `robot` (`BOOLEAN`): Whether the edit was made by a robot (`true`) or a human (`false`).
// MAGIC * `timestamp` (`TIMESTAMP`): The time the edit occurred, as a `java.sql.Timestamp`.
// MAGIC * `user` (`STRING`): The user who made the edit or, if the edit is anonymous, the IP address associated with the anonymous editor.
// MAGIC 
// MAGIC Here are the fields we're ignoring:
// MAGIC 
// MAGIC * `channel` (`STRING`): The Wikipedia IRC channel, e.g., "#en.wikipedia"
// MAGIC * `comment` (`STRING`): The comment associated with the change (i.e., the commit message).
// MAGIC * `delta` (`INT`): The number of lines changes, deleted, and/or added.
// MAGIC * `flag` (`STRING`): A flag indicating the kind of change.
// MAGIC * `namespace` (`STRING`): The page's namespace. See <https://en.wikipedia.org/wiki/Wikipedia:Namespace>
// MAGIC * `newPage` (`BOOLEAN`): Whether or not the edit created a new page.
// MAGIC * `unpatrolled` (`BOOLEAN`): Whether or not the article is patrolled. See <https://en.wikipedia.org/wiki/Wikipedia:New_pages_patrol/Unpatrolled_articles>
// MAGIC * `url` (`STRING`): The URL of the edit diff.
// MAGIC * `userUrl` (`STRING`): The Wikipedia profile page of the user, if the edit is not anonymous.
// MAGIC * `wikipedia` (`STRING`): The readable name of the Wikipedia that was edited (e.g., "English Wikipedia").
// MAGIC * `wikipediaLong` (`STRING`): The long readable name of the Wikipedia that was edited. Might be the same value as the `wikipedia` field.
// MAGIC * `wikipediaShort` (`STRING`): The short name of the Wikipedia that was edited (e.g., "en").
// MAGIC * `wikipediaUrl` (`STRING`): The URL of the Wikipedia edition containing the article.
// MAGIC 
// MAGIC Note that all fields are Scala `Option` types, which indicates that they may or may not actually be present.

// COMMAND ----------

def createContext(): StreamingContext = {
  import org.apache.spark.sql._
  
  val ssc = new StreamingContext(sc, BatchInterval)
  println(s"Connecting to $StreamingServerHost, port $StreamingServerPort")
  val stream = ssc.socketTextStream(StreamingServerHost, StreamingServerPort)

  stream.foreachRDD { rdd =>
    if (! rdd.isEmpty) {
      val sqlContext = SQLContext.getOrCreate(SparkContext.getOrCreate())
      // Convert the RDD to a DataFrame.
      val df = sqlContext.read.json(rdd).coalesce(1)
      df.registerTempTable(DFTableAlias)
      // Append this streamed RDD to our persistent table.
      sqlContext.sql(s"INSERT INTO $TableName SELECT user, anonymous, pageUrl, page, timestamp, robot FROM $DFTableAlias")
    }
  }

  println("Starting context.")
  ssc.start()
  ssc
}

// COMMAND ----------

// MAGIC %md Here are some convenience functions to start and stop the streaming context.

// COMMAND ----------

def stop(): Unit = {
  StreamingContext.getActive.map { ssc =>
    ssc.stop(stopSparkContext=false)
    println("Stopped running streaming context.")
  }
}

/** Convenience function to start our stream.
  */
def start() = {
  StreamingContext.getActiveOrCreate(creatingFunc = createContext _)
}


// COMMAND ----------

// MAGIC %md Okay, let's fire up the streaming context. We're going to let it run for awhile.

// COMMAND ----------

start()

// COMMAND ----------

// MAGIC %md If you want to stop the streaming context, uncomment the code in the following cell and run it.

// COMMAND ----------

//stop()

// COMMAND ----------

// MAGIC %md After about 15 seconds, run the following two cells to verify that data is coming in.

// COMMAND ----------

val df = sqlContext.sql(s"SELECT * FROM $TableName")

// COMMAND ----------

df.count()

// COMMAND ----------

// MAGIC %md Let's take a quick glance at the data.

// COMMAND ----------

display(df)

// COMMAND ----------

// MAGIC %md There are some special pages in there, like "wiki/User:Foo", "wiki/Special:Foo", "wiki/Talk:SomeTopic", etc., that aren't of interest to us. We can filter them out.

// COMMAND ----------

val cleanedDF = df.filter(! ($"pageurl" rlike """wiki/[A-Z].*:"""))

// COMMAND ----------

cleanedDF.count()

// COMMAND ----------

// MAGIC %md
// MAGIC ## A Bit of Visualization

// COMMAND ----------

// MAGIC %md
// MAGIC How many anonymous edits vs. authenticated edits are there? Let's visualize it.
// MAGIC 
// MAGIC For future reference, here's how to configure the pie chart. Select the pie chart, then click on the Plot Options button, and configure the resulting popup as shown here. It's important to select SUM as the aggregation method.
// MAGIC 
// MAGIC ![World Map Plot Options](http://i.imgur.com/rEyOhq3.png)

// COMMAND ----------

display(cleanedDF.groupBy($"anonymous").count())

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Let's use the Databricks pie chart to graph anonymous vs. authenticated vs. robot edits, so we can visualize the percentage of edits made by robots, by authenticated users, and by anonymous users. The `robot` field in the `WikipediaChange` class defines whether the edit was made by a robot or not.
// MAGIC 
// MAGIC We'll use SQL here, but the same solution can be expressed with the DataFrames API.

// COMMAND ----------

cleanedDF.registerTempTable("cleanedDF")
val typed = sqlContext.sql("""|SELECT CASE
                              |WHEN robot = true THEN 'robot'
                              |WHEN anonymous = true THEN 'anon'
                              |ELSE 'logged-in'
                              |END AS type FROM cleanedDF""".stripMargin)
val groupedByType = typed.groupBy("type").count()
display(groupedByType)


// COMMAND ----------

// MAGIC %md
// MAGIC ## Anonymous Edits
// MAGIC 
// MAGIC Let's do some analysis of just the anonymous editors. To make our job a little easier, we'll create another DataFrame containing _just_ the anonymous users.

// COMMAND ----------

val anonDF = cleanedDF.filter($"anonymous" === true)

// COMMAND ----------

anonDF.count()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Visualizing Anonymous Editors' Locations
// MAGIC 
// MAGIC ![World Map](http://i.imgur.com/66KVZZ3.png)
// MAGIC 
// MAGIC Let's see if we can geocode the IP addresses to see where each editor is in the world. As a bonus, we'll plot the geocoded locations on a world map.
// MAGIC 
// MAGIC We'll need a data set that maps IP addresses to their locations in the world. Fortunately, we have one already prepared, built from from the [IP2Location DB5-Lite](http://lite.ip2location.com/databases) database augmented with ISO 3-letter country codes from the [ISO](http://iso.org) web site.
// MAGIC 
// MAGIC Use of the IP2Location dataset requires the following notice: "This site or product includes IP2Location LITE data available from <http://lite.ip2location.com>".

// COMMAND ----------

val dfIP = sqlContext.read.parquet("dbfs:/mnt/training/ip-geocode.parquet")

// COMMAND ----------

dfIP.count()

// COMMAND ----------

// MAGIC %md
// MAGIC We have a DataFrame that contains the anonymous IP addresses. We want to to geocode the IP addresses. But the IP address DataFrame is too big to join against. The simplest solution, and also the most efficient, is to store it in a broadcast variable and do a binary search against it. It generally takes a couple minutes to load the data into memory and broadcast it, but once it's in place, geocoding IP addresses is fairly fast.
// MAGIC 
// MAGIC **NOTES**
// MAGIC 
// MAGIC * We're using a `GeoIPEntry` class (a case class, really) that's defined in a library.
// MAGIC * The binary search function, `geocodeIP()` is also defined in the same library?in fact, several variants of it.
// MAGIC * Loading the geo-IP data and creating the broadcast variable can take a couple minutes. Please be patient.
// MAGIC * We mark the `ipTable` variable as transient (`@transient`) to prevent it from being serialized accidentally by some of the closures. (This can be a problem in notebooks.)

// COMMAND ----------

import com.databricks.training.helpers.InetGeocode.GeoIPEntry
import org.apache.spark.sql.functions._

@transient val ipTable = dfIP.orderBy($"endingIP").collect().map { row =>
  GeoIPEntry(startingIP    = row(0).toString,
             endingIP      = row(1).toString,
             countryCode2  = row(2).toString,
             countryCode3  = row(3).toString,
             country       = row(4).toString,
             stateProvince = row(5).toString,
             city          = row(6).toString,
             latitude      = row(7).asInstanceOf[Double].toFloat,
             longitude     = row(8).asInstanceOf[Double].toFloat
  )
}

// COMMAND ----------

println(s"Created broadcast variable from ${ipTable.length} IPv4 and IPv6 address ranges.")
val ipLookup = sc.broadcast(ipTable)


// COMMAND ----------

// MAGIC %md
// MAGIC Let's take a quick look at the geocoding function, with some test IP addresses, including one RFC-1918 ("for internal use only") IP address. All but the "172.16" address should produce a geolocated result.

// COMMAND ----------

import com.databricks.training.helpers.InetGeocode.InetGeocoder

val geocoder = new InetGeocoder(ipLookup.value)
for (ip <- Vector("69.164.215.38", "66.249.9.74", "192.241.240.249", "172.16.87.2" /* invalid */, "2601:89:4301:95BE:5570:C9AE:1A50:3C09")) {
  geocoder.geocodeIP(ip).map { g => 
    println(s"IP $ip: $g")
  }.
  getOrElse {
    println(s"IP $ip: NO MATCH")
  }
}

// COMMAND ----------

// MAGIC %md
// MAGIC Okay, _now_ we need a way to geocode the IP addresses in our DataFrame. Once again, a UDF solves the problem:

// COMMAND ----------

val uGeocodeIP = sqlContext.udf.register("uGeocodeIP", (ip: String) => {
  if (ip == null)
    None
  else {
    val g = new InetGeocoder(ipLookup.value)
    g.geocodeIP(ip)
  }
})

// COMMAND ----------

// MAGIC %md
// MAGIC _Now_ we can use the UDF to geocode the IP addresses efficiently. We only need one instance of each IP address.

// COMMAND ----------

val geocoded = anonDF.select($"user".as("ipAddress")).groupBy($"ipAddress").count().select($"ipAddress", $"count", uGeocodeIP($"ipAddress").as("geocoded"))


// COMMAND ----------

geocoded.show()

// COMMAND ----------

// MAGIC %md Note that we added a new `geocoded` column, and it's a _structural_ (or nested) data type. Let's take a closer look at the data, to get a sense for where the anonymous editors are. We don't need all the geocoded columns.

// COMMAND ----------

geocoded.printSchema()

// COMMAND ----------

geocoded.select($"geocoded.countryCode3", $"geocoded.country", $"geocoded.stateProvince", $"geocoded.city", $"count").orderBy($"count".desc).show()

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Visualizing the same data on a world map gives us a better sense of the data. Fortunately, the Databricks notebooks support a World Map graph. This graph type requires the 3-letter country codes and the counts, so that's all we're going to extract. While we're at it, let's get rid of the addresses that couldn't be geocoded.

// COMMAND ----------

val geocoded2 = geocoded.select($"count", $"geocoded.countryCode3".as("country")).where(! isnull($"country"))

// COMMAND ----------

// MAGIC %md If you want to assure yourself that our DataFrame is, in fact, growing in size, run this cell:

// COMMAND ----------

geocoded2.count()

// COMMAND ----------

// MAGIC %md Now, we can plot the counts against the world map. Just keep hitting Ctrl-Enter to update the map. Remember: The data only updates every 15 seconds or so. 
// MAGIC 
// MAGIC For future reference, here's how to configure the plot. Select the World Map, then click on the Plot Options button, and configure the resulting popup as shown here. It's important to select COUNT as the aggregation method.
// MAGIC 
// MAGIC ![World Map Plot Options](http://i.imgur.com/wf6BqhZ.png)

// COMMAND ----------

display(geocoded2)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### An exercise for later
// MAGIC 
// MAGIC Remove the United States, Canada, Australia and the UK from the data and re-plot the results, to see who's editing English Wikipedia entries from countries where English is not the primary language.
// MAGIC 
// MAGIC **HINT**: Use the `display()` function. You'll need to arrange the Plot Options appropriately. Examine the Plot Options for the previous map to figure out what to do.

// COMMAND ----------

display( <<<FILL IN>>> )

// COMMAND ----------

// MAGIC %md ## BE SURE TO STOP YOUR STREAM!

// COMMAND ----------

stop()

// COMMAND ----------

