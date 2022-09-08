// Databricks notebook source
// MAGIC %md This example notebook demonstrates how to stream XML data using the auto-loading features of the Spark batch API and the OSS library Spark-XML.
// MAGIC 
// MAGIC All examples can be run after connecting to a cluster.

// COMMAND ----------

val xml2="""<people>
  <person>
    <age born="1990-02-24">25</age>
  </person>
  <person>
    <age born="1985-01-01">30</age>
  </person>
  <person>
    <age born="1980-01-01">30</age>
  </person>
</people>"""

dbutils.fs.put("/FileStore/tables/test/xml/data/age/ages4.xml",xml2)

// COMMAND ----------

val xml3="""<people>
  <person>
    <age born="1990-02-24">25</age>
    <name>Hyukjin</name>
  </person>
  <person>
    <age born="1985-01-01"></age>
  </person>
  <person>
    <age born="1980-01-01">30</age>
  </person>
</people>"""

dbutils.fs.put("/FileStore/tables/test/xml/data/age/ages5.xml",xml3)

// COMMAND ----------

// MAGIC %fs head /FileStore/tables/test/xml/data/age/ages4.xml

// COMMAND ----------

import com.databricks.spark.xml.functions.from_xml
import com.databricks.spark.xml.schema_of_xml
import spark.implicits._
import com.databricks.spark.xml._
import org.apache.spark.sql.functions.{input_file_name}


val toStrUDF = udf((bytes: Array[Byte]) => new String(bytes, "UTF-8")) // UDF to convert the binary to String

val df_schema = spark.read.format("binaryFile").load("/FileStore/tables/test/xml/data/age/").select(toStrUDF($"content").alias("text"))
  
val payloadSchema = schema_of_xml(df_schema.select("text").as[String]) // This is costlier operation when we have too many files because of file-listing schema inference, it is best to use the user-defined custom schema 


// COMMAND ----------

// MAGIC %md 
// MAGIC - `readStream` is used with binary and autoLoader listing mode options enabled.
// MAGIC - `toStrUDF` is used to convert binary data to string format (text).
// MAGIC - `from_xml` is used to convert the string to a complex struct type, with the user-defined schema.

// COMMAND ----------


val df = spark.readStream.format("cloudFiles")
  .option("cloudFiles.useNotifications", "false")
  .option("cloudFiles.format", "binaryFile")
  .load("/FileStore/tables/test/xml/data/age/")
  .select(toStrUDF($"content").alias("text")).select(from_xml($"text", payloadSchema).alias("parsed"))
  .withColumn("path",input_file_name)


// COMMAND ----------

display(df)
