// Databricks notebook source exported at Fri, 17 Jun 2016 13:27:46 UTC
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

// MAGIC %fs ls dbfs:/tmp/pagecounts

// COMMAND ----------

// MAGIC %md How big is the file, in megabytes?

// COMMAND ----------

val dirContents = dbutils.fs.ls("dbfs:/tmp/pagecounts")
val size = dirContents.map(_.size).head / (1024 * 1024)
println(s"$size megabytes\n")

// COMMAND ----------

val path = dirContents.sortWith { (a, b) => a.name > b.name }.head.path

// COMMAND ----------

// MAGIC %md
// MAGIC ### RDDs
// MAGIC  
// MAGIC RDDs can be created by using the Spark Context object's `textFile()` method.

// COMMAND ----------

// MAGIC %md Create an RDD from the recent pagecounts file:

// COMMAND ----------

// Notice that this returns a RDD of Strings
val pagecountsRDD = sc.textFile(path)

// COMMAND ----------

// MAGIC %md There's one partition, because a gzipped file cannot be uncompressed in parallel.

// COMMAND ----------

pagecountsRDD.partitions.length

// COMMAND ----------

// MAGIC %md Let's increase the parallelism by repartitioning. I'm using 6 partitions, which is twice the number of available threads on Databricks Community Edition.

// COMMAND ----------

val pagecountsRDD2 = pagecountsRDD.repartition(6)
pagecountsRDD2.partitions.length

// COMMAND ----------

// MAGIC %md The `count` action counts how many items (lines) total are in the RDD (this requires a full scan of the file):

// COMMAND ----------

val fmt = new java.text.DecimalFormat
println(fmt.format(pagecountsRDD2.count()))

// COMMAND ----------

// MAGIC %md So the count shows that there are several million lines in the file. Notice that the `count()` action took some time to run because it had to read the entire file remotely from S3.

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

// MAGIC %md Let's sum up the request counts per page in the English Wikipiedia, then pull back the top 10. This is a variation of the code on Slide 4.
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

// MAGIC %md Let's remove the special pages. Page like "Talk:Topic", "User:username", "Special:Something", and anything starting with a "." are just cluttering things up. Here's a modification of the above, with a slightly different `filter()` call. 

// COMMAND ----------

val Specials = Set(
  """Category:""".r,
  """Media:""".r,
  """MediaWiki:""".r,
  """Template:""".r,
  """\s+talk:""".r,
  """Help talk:""".r,
  """User:""".r,
  """Talk:""".r,
  """Help:""".r,
  """File""".r,
  """Special:""".r
)
def isSpecialPage(pageTitle: String): Boolean = Specials exists { r => r.findFirstIn(pageTitle).isDefined }

// COMMAND ----------

val pagecountsRDD3 = pagecountsRDD2.flatMap { line =>
  line.split("""\s+""") match {
    case Array(project, page, numRequests, contentSize) => Some((project, page, numRequests.toLong))
    case _ => None
  }
}.
filter { case (project, page, numRequests) => 
  (project == "en") && (! page.startsWith(".")) && (! isSpecialPage(page))
}.
map { case (project, page, numRequests) => (page, numRequests) }.
reduceByKey(_ + _).
sortBy({ case (page, numRequests) => numRequests }, ascending = false)

pagecountsRDD3.take(100).foreach { case (page, totalRequests) => println(s"$page: $totalRequests") }

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

// MAGIC %md Before we move on to DataFrames, let's cache this last RDD and see how much memory it uses.

// COMMAND ----------

pagecountsRDD3.setName("pagecountsRDD2").cache()
val totalPagesRDD = pagecountsRDD3.count() // we need to run an action to fill the cache
println(fmt.format(totalPagesRDD))

// COMMAND ----------

// MAGIC %md 
// MAGIC ## DataFrames
// MAGIC 
// MAGIC Let's try the same thing with DataFrames.
// MAGIC 
// MAGIC To make a DataFrame, we can simply our RDD into another RDD of a different type, something Spark can use to infer a _schema_. Since we have fewer than 23 columns, we'll use a case class.

// COMMAND ----------

object Parser extends Serializable { // This helps with scoping issues in the notebook
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
val pagecountsDF = pagecountsRDD2.flatMap(Parser.parseLine).toDF
  


// COMMAND ----------

pagecountsDF.printSchema()

// COMMAND ----------

pagecountsDF.rdd.partitions.length

// COMMAND ----------

// MAGIC %md Of course, it would be more efficient to read the DataFrame from something other than a gzipped text file. Let's try that, as well. We'll save it to a Parquet file and read it back.

// COMMAND ----------

import org.apache.spark.sql.SaveMode
pagecountsDF.write.mode(SaveMode.Overwrite).parquet("dbfs:/tmp/pagecounts.parquet")

// COMMAND ----------

// MAGIC %fs ls /tmp/pagecounts.parquet

// COMMAND ----------

// Note the use of "spark", not "sqlContext". "spark" is a preinstantiated
// SparkSession, introduced in 2.0.
val pagecountsDFParquet = spark.read.parquet("dbfs:/tmp/pagecounts.parquet")

// COMMAND ----------

pagecountsDFParquet.rdd.partitions.length

// COMMAND ----------

// MAGIC %md Let's get rid of the special pages, as we did with the RDD version. 

// COMMAND ----------

import org.apache.spark.sql.functions._

val uIsSpecialPage = sqlContext.udf.register("isSpecialPage", isSpecialPage _)

val pagecountsDF2 = pagecountsDF.filter($"project" === "en").
                                 filter(! uIsSpecialPage($"pageTitle")).
                                 filter(substring($"pageTitle", 0, 1) !== ".").
                                 groupBy($"pageTitle").
                                 agg(sum($"numberOfRequests").as("count")).
                                 orderBy($"count".desc)
pagecountsDF2.take(100).foreach { row =>
  println(s"${row(0)}: ${row(1)}")
}

// COMMAND ----------

// MAGIC %md Easier to read, but... not type-safe. 

// COMMAND ----------

// MAGIC %md 
// MAGIC ### A very brief aside, about partitions
// MAGIC 
// MAGIC According to Spark documentation, when a DataFrame shuffle occurs, the number of post-shuffle partitions is defined by Spark configuration parameter 
// MAGIC `spark.sql.shuffle.partitions`, which defaults to 200. So, we should have that many partitions in `pagecountsDF2`, because the `groupBy` and `agg` calls, above, are likely to produce shuffles.
// MAGIC 
// MAGIC But...

// COMMAND ----------

println(s"spark.sql.shuffle.partitions = ${sqlContext.getConf("spark.sql.shuffle.partitions")}")
println(s"pagecountsDF2 partitions     = ${pagecountsDF2.rdd.partitions.length}")

// COMMAND ----------

// MAGIC %md Okay, that's not 200.
// MAGIC 
// MAGIC If we reran the creation of `pagecountsDF2`, above, we might see 20, 21 or 22 for the number of post-shuffle partitions. Why?
// MAGIC 
// MAGIC It turns out that the `orderBy`, above, uses _range partitioning_. To determine reasonable upper and lower bounds for the range, Spark randomly samples the data. In this case, we end up with something around 22 partitions; the partition could might differ with different data. It's not always the same from run to run because the random sampling doesn't use the same seed every time.

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Back to our DataFrame
// MAGIC 
// MAGIC The `pagecountsDF2` DataFrame consists of `Row` objects, which can contain heterogenous types. If we pull some `Rows` back to the driver, we can extract the columns, but only as type `Any`:

// COMMAND ----------

val first10Rows = pagecountsDF2.take(10)

// COMMAND ----------

val row = first10Rows.head
row(0)

// COMMAND ----------

// MAGIC %md Note that `Row` isn't typed. It can't be, since each row consists of columns of potentially different types. (I suppose Spark could've used Shapeless, but it didn't...) If we want to get back to actual, useful Scala types, we have to do something ugly, like this:

// COMMAND ----------

first10Rows.map { row =>
  (row(0).asInstanceOf[String], row(1).asInstanceOf[Long])
}

// COMMAND ----------

// MAGIC %md Before we move on to Datasets, let's:
// MAGIC * verify that the number of items in the DataFrame match the RDD with the special pages filtered out
// MAGIC * cache the DataFrame 
// MAGIC * compare the cached size to the cached size of the RDD.

// COMMAND ----------

val totalPagesDF = pagecountsDF2.cache().count()
println(s"RDD total: ${fmt.format(totalPagesRDD)}")
println(s"DF total:  ${fmt.format(totalPagesDF)}")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Datasets
// MAGIC The easiest way to create a Dataset from scratch is from a DataFrame. Prior to Spark 2.0, we'd use the `SQLContext` for that:
// MAGIC 
// MAGIC ```
// MAGIC val ds = sqlContext.read.text("dbfs:/tmp/pagecounts").as[String]
// MAGIC          |------------- makes a DataFrame ----------|
// MAGIC                                                       |--------|
// MAGIC                                                            |
// MAGIC                                                            +- converts to a Dataset
// MAGIC ```
// MAGIC 
// MAGIC However, in 2.0, while that approach still works, we should prefer to use the `SparkSession`, available as `spark` in these notebooks and in the `spark-shell` Scala REPL.

// COMMAND ----------

// Notice that this returns a Dataset of Strings
val pagecountsDS = spark.read.text("dbfs:/tmp/pagecounts/").as[String]
pagecountsDS.take(3).foreach(println)

// COMMAND ----------

// MAGIC %md Of course, we could also just convert the existing `pagecountsDF` to a Dataset:

// COMMAND ----------

val pagecountsDS2 = pagecountsDF.as[(String, String, Long)]
pagecountsDS2.take(3).foreach(println)

// COMMAND ----------

// MAGIC %md Even better, though, let's make a Dataset that uses something more convenient than a tuple:

// COMMAND ----------

// Matching is done DataFrame column name -> case class field name
case class Edit(project: String, pageTitle: String, numberOfRequests: Long)
val pagecountsDS3 = pagecountsDF.as[Edit]

// COMMAND ----------

pagecountsDS3.take(4).foreach(println)

// COMMAND ----------

println(fmt.format(pagecountsDS3.count()))

// COMMAND ----------

// MAGIC %md ### I lied (a little)
// MAGIC 
// MAGIC Prior to 2.0, `DataFrame` and `Dataset` were two different types. In 2.0, though, a `DataFrame` is just a type alias for `Dataset[Row]`. Thus, in 2.0,
// MAGIC when you start with a `DataFrame` and "convert" it to a `Dataset`, you're actually converting a `Dataset` of one type to a `Dataset` of another type.

// COMMAND ----------

pagecountsDF.getClass

// COMMAND ----------

// MAGIC %md You still have to get the types right on the conversions to Datasets, but once you have the Dataset, you have something that's type safe again. 
// MAGIC 
// MAGIC Once again, let's filter out the special pages, group by page title, and show to top 100 hits.

// COMMAND ----------

val pagecountsDS4 = pagecountsDS3.filter { e => (e.project == "en") && (! e.pageTitle.startsWith(".")) && (! isSpecialPage(e.pageTitle)) }.
                                  groupByKey { _.pageTitle }.  // GroupedDataset[String, DSEntry]
                                  reduceGroups { (e1, e2) => e1.copy(e1.project, e1.pageTitle, e1.numberOfRequests + e2.numberOfRequests) }.
                                  map(_._2). // skip the key; extract the value
                                  orderBy($"numberOfRequests".desc)

pagecountsDS4.take(100).foreach { e => println(s"${e.pageTitle}: ${e.numberOfRequests}") }
              

// COMMAND ----------

// MAGIC %md Let's cache this Dataset and, for good measure, compare the number of items with our RDD and DataFrame.

// COMMAND ----------

val totalPagesDS = pagecountsDS4.cache().count()
println(s"DF total:  ${fmt.format(totalPagesDF)}")
println(s"RDD total: ${fmt.format(totalPagesRDD)}")
println(s"DS total:  ${fmt.format(totalPagesDS)}")

// COMMAND ----------

pagecountsDS4.rdd.partitions.length

// COMMAND ----------

// MAGIC %md
// MAGIC ### The last little bit of caching
// MAGIC 
// MAGIC So far, we have cached:
// MAGIC 
// MAGIC * A filtered RDD of `(pageTitle, totalRequests)` tuples
// MAGIC * A DataFrame of `Row` objects
// MAGIC * A Dataset of `Edit` objects

// COMMAND ----------

// MAGIC %md Just for completeness, let's cache an RDD with `Edit` objects. Given the small amount of memory available to Community Edition clusters, we'll allow this cache to spill to disk if it has to.

// COMMAND ----------

import org.apache.spark.storage.StorageLevel
val pagecountsEditRDD = pagecountsRDD2.flatMap { line =>
  line.split("""\s+""") match {
    case Array(project, pageTitle, requests, _) => Some(Edit(project, pageTitle, requests.toLong))
    case _ => None
  }
}.
filter { e => (e.project == "en") && (! e.pageTitle.startsWith(".")) && (! isSpecialPage(e.pageTitle)) }
pagecountsEditRDD.setName("pagecountsEditRDD").persist(StorageLevel.MEMORY_AND_DISK).count()

// COMMAND ----------

// MAGIC %md The Spark UI's Storage tab now shows all of these in memory. The Dataset is compressed in memory by default, so it takes up much less space.

// COMMAND ----------

// MAGIC %md ## END OF DEMO

// COMMAND ----------


