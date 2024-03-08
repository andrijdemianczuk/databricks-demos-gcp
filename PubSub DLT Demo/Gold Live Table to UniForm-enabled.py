# Databricks notebook source
#Open the stream from the live table and write it to the UC location

(spark.readStream
      .format("delta")
      #.option("ignoreChanges", "true")
      .table("ademianczuk.telus_netscout_poc.pubsub_gold_terminated_messages")
      .writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", "/FileStore/tmp/checkpoints/netscout")
      .toTable("ademianczuk.telus_netscout_poc.pubsub_gold_teminated_iceberg")
)

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/Users/andrij.demianczuk@databricks.com"))
