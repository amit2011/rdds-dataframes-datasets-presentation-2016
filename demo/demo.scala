// Databricks notebook source exported at Fri, 4 Mar 2016 03:26:05 UTC
// MAGIC %md
// MAGIC 
// MAGIC #![Wikipedia Logo](http://sameerf-dbc-labs.s3-website-us-west-2.amazonaws.com/data/wikipedia/images/w_logo_for_labs.png)
// MAGIC 
// MAGIC # RDDs, DataFrames and Datasets
// MAGIC 
// MAGIC ## Using Wikipedia data
// MAGIC 
// MAGIC We're going to use some Wikipedia data, representing changes to various Wikimedia project pages in an hour's time. These edits are from March 3rd (yesterday), covering the hour starting at 22:00 UTC.
// MAGIC 
// MAGIC Dataset: https://dumps.wikimedia.org/other/pagecounts-raw/

// COMMAND ----------

// MAGIC %fs ls dbfs:/mnt/training-write/essentials/pagecounts/staging/

// COMMAND ----------

// MAGIC %md How big is the file, in megabytes?

// COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/training-write/essentials/pagecounts/staging/").map(_.size).head / (1024 * 1024)

// COMMAND ----------

// MAGIC %md
// MAGIC ### RDDs
// MAGIC RDDs can be created by using the Spark Context object's `textFile()` method.

// COMMAND ----------

// MAGIC %md Create an RDD from the recent pagecounts file:

// COMMAND ----------

// Notice that this returns a RDD of Strings
val pagecountsRDD = sc.textFile("dbfs:/mnt/training-write/essentials/pagecounts/staging/pagecounts-20160303-220000.gz")

// COMMAND ----------

// MAGIC %md There's one partition, because a gzipped file cannot be uncompressed in parallel.

// COMMAND ----------

pagecountsRDD.partitions.length

// COMMAND ----------

// MAGIC %md Let's increase the parallelism by repartitioning.

// COMMAND ----------

val pagecountsRDD2 = pagecountsRDD.repartition(16)
pagecountsRDD2.partitions.length

// COMMAND ----------

// MAGIC %md The `count` action counts how many items (lines) total are in the RDD (this requires a full scan of the file):

// COMMAND ----------

pagecountsRDD2.count()

// COMMAND ----------

// MAGIC %md So the count shows that there are aalmost 7.5 million lines in the file. Notice that the `count()` action took 15 - 25 seconds to run because it had to read the entire file remotely from S3.

// COMMAND ----------

// MAGIC %md Let's take a look at some of the data. This will return faster, because `take()` doesn't have to process the whole file.

// COMMAND ----------

pagecountsRDD2.take(10).foreach(println)

// COMMAND ----------

// MAGIC %md Notice that each line in the file actually contains 2 strings and 2 numbers, but our RDD is treating each line as a long string. We'll fix this typing issue shortly by using a custom parsing function.

// COMMAND ----------

// MAGIC %md In the output above, the first column (like `aa`) is the Wikimedia project name. The following abbreviations are used for the first column:
// MAGIC 
// MAGIC * wikibooks: `".b"`
// MAGIC * wiktionary: `".d"`
// MAGIC * wikimedia: `".m"`
// MAGIC * wikipedia mobile: `".mw"`
// MAGIC * wikinews: `".n"`
// MAGIC * wikiquote: `".q"`
// MAGIC * wikisource: `".s"`
// MAGIC * wikiversity: `".v"`
// MAGIC * mediawiki: `".w"`
// MAGIC 
// MAGIC Projects without a period and a following character are Wikipedia projects. So, any line starting with the column `aa` refers to the Aragones language Wikipedia. Similarly, any line starting with the column `en` refers to the English language Wikipedia. `en.b` refers to English Language Wikibooks.
// MAGIC 
// MAGIC The second column is the title of the page retrieved, the third column is the number of requests, and the fourth column is the size of the content returned.

// COMMAND ----------

// MAGIC %md Let's cache the RDD, so we're not reading the gzipped file over and over again.

// COMMAND ----------

pagecountsRDD2.setName("pagecountsRDD").cache().count() // we need to run an action to fill the cache

// COMMAND ----------

// MAGIC %md Let's sum up the request counts per page in the English Wikipiedia, then pull back the top 10. This is a variation of the code on Slide 5.
// MAGIC   

// COMMAND ----------

pagecountsRDD2.flatMap { line =>
  line.split("""\s+""") match {
    case Array(project, page, numRequests, contentSize) => Some((project, page, numRequests.toLong))
    case _ => None
  }
}.
filter { case (project, page, numRequests) => project == "en" }.
map { case (project, page, numRequests) => (page, numRequests) }.
reduceByKey(_ + _).
sortBy({ case (page, numRequests) => numRequests }, ascending = false).
take(100).
foreach { case (page, totalRequests) => println(s"$page: $totalRequests") }

// COMMAND ----------

// MAGIC %md Let's remove the special pages. Page like "Talk:Topic", "User:username", "Special:Something", and anything starting with a "." are just cluttering things up. Here's a modification of the above, with
// MAGIC a slightly modified `filter()` call.

// COMMAND ----------

pagecountsRDD2.flatMap { line =>
  line.split("""\s+""") match {
    case Array(project, page, numRequests, contentSize) => Some((project, page, numRequests.toLong))
    case _ => None
  }
}.
filter { case (project, page, numRequests) => 
  (project == "en") && (! page.contains(":")) && (! page.startsWith("."))
}.
map { case (project, page, numRequests) => (page, numRequests) }.
reduceByKey(_ + _).
sortBy({ case (page, numRequests) => numRequests }, ascending = false).
take(100).
foreach { case (page, totalRequests) => println(s"$page: $totalRequests") }

// COMMAND ----------

// MAGIC %md That's completely type-safe, but it's up to us to choose the right implementation. For instance, the code, above, _could_ have by done like this:
// MAGIC 
// MAGIC ```
// MAGIC pagecountsParsedRDD2.flatMap { ... }.
// MAGIC                      filter { ... }.
// MAGIC                      map { case (project, page, numRequests) => (page, numRequests) }.
// MAGIC                      groupByKey(_._1).
// MAGIC                      reduce(_ + _).
// MAGIC                      ...
// MAGIC ```
// MAGIC 
// MAGIC However, `groupByKey() + reduce()` is _far_ more inefficient than `reduceByKey()`. Yet, Spark cannot protect us from choosing the wrong transformations.

// COMMAND ----------

// MAGIC %md 
// MAGIC ## DataFrames
// MAGIC 
// MAGIC Let's try the same thing with DataFrames.
// MAGIC 
// MAGIC To make a DataFrame, we'll need to convert our RDD into another RDD of a different type, something Spark can use to infer a _shema_. We'll use a case class.

// COMMAND ----------

class Parser extends Serializable { // This helps with scoping issues in the notebook
  case class EditEntry(project: String, pageTitle: String, numberOfRequests: Long)

  def parseLine(line: String) = {
    line.split("""\s+""") match {
      case Array(project, page, numRequests, _) =>
        Some(EditEntry(project, page, numRequests.toLong))
      case _ =>
        None
    }
  }
}
val pagecountsDF = pagecountsRDD2.flatMap((new Parser).parseLine).toDF
  


// COMMAND ----------

// MAGIC %md Let's do what we did with the RDD, only with the DataFrame. This is an adaptation of what's on Slide 7.

// COMMAND ----------

import org.apache.spark.sql.functions._

val top100DF = pagecountsDF.filter($"project" === "en").
                            filter(locate(":", $"pageTitle") === 0).
                            filter(substring($"pageTitle", 0, 1) !== ".").
                            groupBy($"pageTitle").
                            agg(sum($"numberOfRequests").as("count")).
                            orderBy($"count".desc)
top100DF.take(100).foreach { row =>
  println(s"${row(0)}: ${row(1)}")
}

// COMMAND ----------

// MAGIC %md Easier to read, but... not type-safe. Yuck.

// COMMAND ----------

top100DF.take(1)

// COMMAND ----------

// MAGIC %md If I want to get back to actual, useful Scala types, I have to do something like this:

// COMMAND ----------

top100DF.take(2).map { row =>
  (row(0).asInstanceOf[String], row(1).asInstanceOf[Long])
}

// COMMAND ----------

// MAGIC %md
// MAGIC ## Datasets
// MAGIC Datasets can be created by using the SQL Context object's `read.text()` method:

// COMMAND ----------

// Notice that this returns a Dataset of Strings
val pagecountsDS = sqlContext.read.text("dbfs:/mnt/wikipedia-readwrite/pagecounts/staging/").as[String]

// COMMAND ----------

// MAGIC %md Or, you can convert a DataFrame into a Dataset.

// COMMAND ----------

val pagecountsDS2 = pagecountsDF.as[(String, String, Long)]
pagecountsDS2.take(3).foreach(println)

// COMMAND ----------

// MAGIC %md Or:

// COMMAND ----------

// Matching is done DataFrame column name -> case class field name
case class Edit(project: String, pageTitle: String, numberOfRequests: Long)
val pagecountsDS3 = pagecountsDF.as[Edit]

// COMMAND ----------

pagecountsDS3.count()

// COMMAND ----------

pagecountsDS3.take(3).foreach(println)

// COMMAND ----------

// MAGIC %md You still have to get the types right on the conversions to Datasets, but once you have the Dataset, you have something that's type safe again. 

// COMMAND ----------

pagecountsDS3.filter(e => (e.project == "en") && (! e.pageTitle.contains(":")) && (! e.pageTitle.startsWith("."))).
              groupBy(_.pageTitle).  // GroupedDataset[String, DSEntry]
              reduce { (e1, e2) =>
                e1.copy(e1.project, e1.pageTitle, e1.numberOfRequests + e2.numberOfRequests)
              }.
              map(_._2).
              toDF. // because there's currently no sortBy or orderBy on a Dataset
              orderBy($"numberOfRequests".desc).
              as[Edit].
              take(100).
              foreach { e =>
                println(s"${e.pageTitle}: ${e.numberOfRequests}")
              }
              

// COMMAND ----------

// MAGIC %md
// MAGIC ### Caching RDDs vs Datasets in memory
// MAGIC 
// MAGIC The `pagecountsRDD2` is already cached in memory. Let's cache the non-derived Dataset, as well, and compare them.

// COMMAND ----------

pagecountsDS.cache().count()

// COMMAND ----------

// MAGIC %md And what happens if we cache a Dataset with Edit entries vs. an RDD with Edit entries?

// COMMAND ----------

val pagecountsParsedDS = pagecountsDS.map(parseLine)
pagecountsParsedDS.cache().count()

// COMMAND ----------

// MAGIC %md The Spark UI's Storage tab now shows both in memory. The Dataset is compressed in memory by default, so it takes up much less space.

// COMMAND ----------

// MAGIC %md
// MAGIC ### A little more caching

// COMMAND ----------

// MAGIC %md We'll now cache our parsed RDD, and we'll make a parsed Dataset to cache, as well.

// COMMAND ----------

pagecountsParsedRDD.setName("pagecountsParsedRDD").cache().count()

// MAGIC %md This concludes the lab.
