# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://www.cdata.com/ui/img/logo-googlecloudstorage.png" />
# MAGIC 
# MAGIC ## Introduction
# MAGIC In this series, we will be connecting to a GCS bucket and loading some XML data in systematically for some basic ETL. This is a common use-case and although XML isn't as widely used as other storage formats it is still considered powerful with it's recursion and nesting capabilities.
# MAGIC 
# MAGIC [Getting Started With GCS on Databricks](https://docs.gcp.databricks.com/data/data-sources/google/gcs.html)
# MAGIC 
# MAGIC ## Part 3: Enriching the data
# MAGIC In this notebook we will be creating some enrichments to our data by aggregating and truncating some of the data points for ease-of-access and readability. These will be considered our 'gold' tables and can be used for BI/BA

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as f
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Variables
database = "ademianczuk"

# COMMAND ----------

# DBTITLE 1,Load detail data and process the dataframe
df = spark.table(f"{database}.s_purchaseorders_detail")
df.count() #Used to commit the dataframe to memory
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Cache the table
df.count()
df.cache()

# COMMAND ----------

# DBTITLE 1,Preview the table
display(df)

# COMMAND ----------

# DBTITLE 1,Window by part number
windowPart = Window.partitionBy("_PartNumber").orderBy("_PartNumber")
windowPartAgg = Window.partitionBy("_PartNumber")

df2 = df.withColumn("row", f.row_number().over(windowPart)) \
  .withColumn("avg", f.avg(f.col("Quantity")).over(windowPartAgg)) \
  .withColumn("sum", f.sum(f.col("Quantity")).over(windowPartAgg)) \
  .withColumn("min", f.min(f.col("Quantity")).over(windowPartAgg)) \
  .withColumn("max", f.max(f.col("Quantity")).over(windowPartAgg)) \
  .withColumn("stddev", f.stddev(f.col("Quantity")).over(windowPartAgg)) \
  .where(f.col("row")==1).select("_PartNumber","ProductName","avg","sum","min","max","stddev")

# COMMAND ----------

delta_loc_po = "dbfs:/FileStore/Users/andrij.demianczuk@databricks.com/tmp/PurchaseOrders/po_purchase_agg"
write_fmt = 'delta'
table_name = 'g_purchaseorders_pAgg'
write_mode = 'append'
#partition_by = "_PurchaseOrderNumber"
database = "ademianczuk"

# COMMAND ----------

# DBTITLE 1,Commit to delta files & table
df2.write \
  .format(write_fmt) \
  .mode(write_mode) \
  .save(delta_loc_po)

#spark.sql(f"DROP TABLE IF EXISTS {database}.{table_name}")
#spark.sql("CREATE TABLE " + database + "."+ table_name + " USING DELTA LOCATION '" + delta_loc + "'")

if spark._jsparkSession.catalog().tableExists(database, table_name):

  print("table already exists..... appending data")
  
  # Create a view and merge. Since this is a large table with no unique identifiers, this is only here for example.
  df2.createOrReplaceTempView("vw_po_pAgg")  
  
  spark.sql(f"MERGE INTO {database}.{table_name} USING vw_po_pAgg \
    ON vw_po_pAgg._PartNumber = {database}.{table_name}._PartNumber \
    WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
  
else:
  print("table does not exist..... creating a new one")
  # Write the data to its target.
  spark.sql("CREATE TABLE " + database + "."+ table_name + " USING DELTA LOCATION '" + delta_loc_po + "'")
