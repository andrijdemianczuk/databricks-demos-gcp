// Databricks notebook source
display(dbutils.fs.ls("/mnt/ademianczuk-1/data/xml"))

// COMMAND ----------

import com.databricks.spark.xml.functions.from_xml
import com.databricks.spark.xml.schema_of_xml
import spark.implicits._
import com.databricks.spark.xml._
import org.apache.spark.sql.functions.{input_file_name}

// UDF to convert the binary to String
val toStrUDF = udf((bytes: Array[Byte]) => new String(bytes, "UTF-8"))

//Identify the schema of the collective files in the target directory. This only works if all files being read-in have the same structure.
val df_schema = spark.read.format("binaryFile").load("/mnt/ademianczuk-1/data/xml").select(toStrUDF($"content").alias("text"))
  
// This is costlier operation when we have too many files because of file-listing schema inference, it is best to use the user-defined custom schema 
val payloadSchema = schema_of_xml(df_schema.select("text").as[String])


// COMMAND ----------

val df = spark.readStream.format("cloudFiles")
  .option("cloudFiles.useNotifications", "false")
  .option("cloudFiles.format", "binaryFile")
  .load("/mnt/ademianczuk-1/data/xml/")
  .select(toStrUDF($"content").alias("text")).select(from_xml($"text", payloadSchema).alias("parsed"))
  .withColumn("path",input_file_name)

// COMMAND ----------

display(df)

// COMMAND ----------


