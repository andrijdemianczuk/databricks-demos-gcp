// Databricks notebook source
// MAGIC %md
// MAGIC # Google Cloud Storage microbatching demo

// COMMAND ----------

// MAGIC %md
// MAGIC <img src="https://www.cdata.com/ui/img/logo-googlecloudstorage.png" />
// MAGIC 
// MAGIC ## Introduction
// MAGIC In this series, we will be connecting to a GCS bucket and loading some XML data in systematically for some basic ETL. This is a common use-case and although XML isn't as widely used as other storage formats it is still considered powerful with it's recursion and nesting capabilities.
// MAGIC 
// MAGIC [Getting Started With GCS on Databricks](https://docs.gcp.databricks.com/data/data-sources/google/gcs.html)
// MAGIC 
// MAGIC ## Part 1: Accessing and loading the data
// MAGIC We will begin with the following assumptions:
// MAGIC 1. The Databricks Cluster is being run by a service account with access to Google Cloud Storage objects
// MAGIC 1. The GCS master object has been mounted to /mnt

// COMMAND ----------

// MAGIC %md
// MAGIC ### Known limitations
// MAGIC Since we're going to be serializing the XML files into a binary format when read, there is a limit to the size of XML file that will work with this design - files must be within 1.99GB limit. If files exceed this threshold, they will first need to be chunked up before they can be parsed.

// COMMAND ----------

// DBTITLE 1,Variable Init
val bucket_name="ademianczuk-1"
val _root="/mnt/"
val data_loc="/data/xml/"
val _path=_root+bucket_name+data_loc
val username="ademianczuk"
val table_name = "ademianczuk.b_PurchaseOrders"
val checkpoint_path = "/tmp/ademianczuk/_checkpoint"

// COMMAND ----------

// DBTITLE 1,Testing GCS connectivity
//DBUtils allow us to have a peek into our directory structure. This will be handy when we need to reference the source directory of the bucket where we are storing our files.
display(dbutils.fs.ls(_path))

// COMMAND ----------

// DBTITLE 1,Define & Compile the XML schema from a sample file
import com.databricks.spark.xml.functions.from_xml
import com.databricks.spark.xml.schema_of_xml
import spark.implicits._
import com.databricks.spark.xml._
import org.apache.spark.sql.functions.{input_file_name}

// UDF to convert the binary to String
val toStrUDF = udf((bytes: Array[Byte]) => new String(bytes, "UTF-8"))

//Identify the schema of the collective files in the target directory. This only works if all files being read-in have the same structure.
//IMPORTANT: If you read all of the files in the source bucket, this can take a while. Here I am using an analog of the xml file just to build the schema.
//val df_schema = spark.read.format("binaryFile").load("dbfs:/FileStore/Users/andrij.demianczuk@databricks.com/pos.xml").select(toStrUDF($"content").alias("text"))
val df_schema = spark.read.format("binaryFile").load(_path).select(toStrUDF($"content").alias("text"))
  
// This is costlier operation when we have too many files because of file-listing schema inference, it is best to use the user-defined custom schema. This is why we're just going to opt to use a single example file.
val payloadSchema = schema_of_xml(df_schema.select("text").as[String])

// COMMAND ----------

// DBTITLE 1,**Clearing out a previous run and starting over
// Clear out data from previous demo execution. This can be un-commented if you wish to create a new table and checkpoint from scratch
// spark.sql(s"DROP TABLE IF EXISTS ${table_name}")
// dbutils.fs.rm(checkpoint_path, true)

// COMMAND ----------

// DBTITLE 1,Read the files and update the table
//IMPORTANT! This cell *has* to be the last one in the notebook. If we're using StreamReader, then the following cell will run before this one has completed.

// Imports
import org.apache.spark.sql.functions.{current_timestamp}
import org.apache.spark.sql.streaming.Trigger

// Configure Auto Loader to ingest JSON data to a Delta table. We're going to use a trigger instead of a stream to ingest everything since the last run
spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "binaryFile")
  .load(_path)
  .select(toStrUDF($"content").alias("text")).select(from_xml($"text", payloadSchema).alias("payload"))
  .writeStream
  .option("checkpointLocation", checkpoint_path)
  .trigger(Trigger.AvailableNow) //<-- Use this for a trigger-once. On subsequent runs, new data will get added
  .option("mergeSchema", "true") //<-- This makes sure schema conflicts are resolved
  .outputMode("Append") //<-- This is what keeps things running smooth and fast in tandem with the trigger and checkpoint location
  .toTable(table_name)
