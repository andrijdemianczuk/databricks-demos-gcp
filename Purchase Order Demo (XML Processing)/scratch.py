# Databricks notebook source
# MAGIC %sql
# MAGIC DESCRIBE DETAIL ademianczuk.dlt_purchaseorders_details

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL ademianczuk.dlt_purchaseorders_raw

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS ademianczuk.dlt_purchaseorders_details")
dbutils.fs.rm("dbfs:/pipelines/de4d6b5c-1636-44ba-87b9-ee2db3376ec6/tables/dlt_purchaseorders_details", True)

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS ademianczuk.dlt_purchaseorders_raw")
dbutils.fs.rm("dbfs:/pipelines/de4d6b5c-1636-44ba-87b9-ee2db3376ec6/tables/dlt_purchaseorders_raw", True)
