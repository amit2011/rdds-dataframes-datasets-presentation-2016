// Databricks notebook source exported at Thu, 16 Jun 2016 20:18:02 UTC
// MAGIC %md 
// MAGIC # Download the Wikipedia Pagecounts Data File
// MAGIC 
// MAGIC Use this notebook to download the latest Wikipedia pagecounts data file.

// COMMAND ----------

import java.text.SimpleDateFormat
import java.util.Calendar
import java.net.URL
import java.io.File
import sys.process._
import scala.language.postfixOps
import scala.util.matching.Regex
import scala.collection.mutable
import scala.io.Source

// COMMAND ----------

// MAGIC %md #### Decide what the latest hourly pagecounts file is:

// COMMAND ----------

// MAGIC %md The function in the next cell will:
// MAGIC * Check the current year and month locally
// MAGIC * Go to wikimedia.org to download the webpage (html file) for the current month's file dumps to the local Driver container
// MAGIC * Parse the downloaded webpage and find the latest file to download
// MAGIC * Return the URL for the latest file to download

// COMMAND ----------

val TodaysDumpsURL = {
  val today = new java.util.Date
  val yearFormat = new SimpleDateFormat("y")
  val year = yearFormat.format(today)
  val monthFormat = new SimpleDateFormat("MM")
  val month = monthFormat.format(today)
  s"https://dumps.wikimedia.org/other/pagecounts-raw/$year/$year-$month"
}

// COMMAND ----------

// Define a function that figures out what the latest file is
def decideLatestFile():String = {

  // Read the local file into String currentPagecountsWebpage
  val source = Source.fromURL(TodaysDumpsURL)
  val currentPagecountsWebpage = try source.mkString finally source.close()
  
  // Define a regex pattern and apply it to currentPagecountsWebpage
  val pattern = """<a href="[^"]+">pagecounts-(\d+)-(\d+)\.gz</a>""".r
  val pagecountNames = (pattern findAllMatchIn currentPagecountsWebpage).map { m => (m.group(1), m.group(2)) }

  val newest = pagecountNames.maxBy { case (date, hour) => 
    s"$date$hour".toLong
  }
  
  // Construct a URL for the latest file to download and return it
  TodaysDumpsURL + "/" + "pagecounts-" + newest._1 + "-" + newest._2 + ".gz" //newestFile.toString.drop(7)
}

// COMMAND ----------

// MAGIC %md Call the decideLatestFile() function and store the returned URL string in value 'url':

// COMMAND ----------

val url = decideLatestFile()

// COMMAND ----------

Source.fromURL(TodaysDumpsURL).getLines.take(20).foreach(println)

// COMMAND ----------

// MAGIC %md #### Download the latest pagecounts file to a shared S3 staging folder:

// COMMAND ----------

// MAGIC %md First, check which hour's pagecount file is currently in the staging folder:

// COMMAND ----------

val StagingDir = "dbfs:/tmp/pagecounts"

// COMMAND ----------

// MAGIC %fs rm --recurse=true "/tmp/pagecounts"

// COMMAND ----------

// MAGIC %fs mkdirs "/tmp/pagecounts"

// COMMAND ----------

// Define a function that downloads the latest file to DBFS
def downloadLatestFile(url:String) = {
  val baseFile = url.split("/").last
  val temp = s"/tmp/$baseFile"

  // Clear target directory/bucket
  try {
    dbutils.fs.ls(StagingDir).foreach(f => dbutils.fs.rm(f.path, recurse=false))
  }
  catch {
    case _: java.io.FileNotFoundException => // don't worry about it
  }
  
  // Download the file to the Driver's local file system
  new URL(url) #> new File(temp) !!
  
  // Copy the file from the Driver's file system to S3
  dbutils.fs.cp(s"file://$temp", s"$StagingDir/$baseFile")
  
  // Remove the local temporary file.
  //s"rm $temp" !!
  
  println(s"Sucessfully downloaded: $baseFile")
}

// COMMAND ----------

// MAGIC %md This download should take about 1-2 minutes to complete:

// COMMAND ----------

downloadLatestFile(url)

// COMMAND ----------

display(dbutils.fs.ls(StagingDir))

// COMMAND ----------


