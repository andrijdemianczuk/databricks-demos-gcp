// Databricks notebook source
// MAGIC %md
// MAGIC # Sample read of XML
// MAGIC ## Part 1: Reading
// MAGIC First, let's test to see if our GCS bucket is accessible and we can read the XML files we're generating with pyEventGen

// COMMAND ----------

// MAGIC %python
// MAGIC #Test connectivity to the bucket
// MAGIC dbutils.fs.ls("/mnt/ademianczuk-1/data/xml")

// COMMAND ----------

// MAGIC %scala
// MAGIC import com.databricks.spark.xml._
// MAGIC import com.databricks.spark.xml.functions.from_xml
// MAGIC import com.databricks.spark.xml.schema_of_xml
// MAGIC import spark.implicits._
// MAGIC 
// MAGIC //We're going to start first by parsing out each PO. The nested XML will happen down the road
// MAGIC val df = spark.read.format("com.databricks.spark.xml").option("rowTag", "PurchaseOrder").xml("dbfs:/mnt/ademianczuk-1/data/xml/")

// COMMAND ----------

//let's have a quick look at the contents of our dataframe
display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Part 2: Streaming?

// COMMAND ----------

import com.databricks.spark.xml._
import com.databricks.spark.xml.functions.from_xml
import com.databricks.spark.xml.schema_of_xml
import spark.implicits._

val df2 = (spark.readStream.format("cloudFiles").option("cloudFiles.format", "text").load("dbfs:/mnt/ademianczuk-1/data/xml/*.xml"))

// COMMAND ----------

display(df2)

// COMMAND ----------


