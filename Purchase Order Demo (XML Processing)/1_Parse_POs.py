# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://www.cdata.com/ui/img/logo-googlecloudstorage.png" />
# MAGIC 
# MAGIC ## Introduction
# MAGIC In this series, we will be connecting to a GCS bucket and loading some XML data in systematically for some basic ETL. This is a common use-case and although XML isn't as widely used as other storage formats it is still considered powerful with it's recursion and nesting capabilities.
# MAGIC 
# MAGIC [Getting Started With GCS on Databricks](https://docs.gcp.databricks.com/data/data-sources/google/gcs.html)
# MAGIC 
# MAGIC ## Part 2: Parsing the XML ingest data
# MAGIC In the previous notebook, we read in our XML files incrementally with a triggered stream. This means that with every run, any new files are read in as a binary file, and added to the table 'b_PurchaseOrders'. Now we need to break those elements out into a series of new tables.

# COMMAND ----------

df = spark.table("default.b_PurchaseOrders")
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Preview the bronze table in it's original state
display(df)

# COMMAND ----------

# DBTITLE 1,Deal with nested structure (level 1)
#The first thing we need to do is extract the Purchase Order from the payload struct. Once done, we'll drop the payload column to reduce the size of the dataframe
df = df.select("payload.*","*")
df = df.drop(df.payload)
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Explode the PurchaseOrder array (to get one row for each order)
#Our first nested structure is an array so we'll explode each PurchaseOrder out to it's own row with the explode() function
from pyspark.sql.functions import explode
df = df.select(explode(df.PurchaseOrder))
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Build the first iteration of the silver table
#rinse and repeat the process
#The first thing we need to do is extract the Purchase Order from the payload struct. Once done, we'll drop the payload column to reduce the size of the dataframe
df = df.select("col.*","*")
df = df.drop(df.col)
df.printSchema()

# COMMAND ----------

display(df)

# COMMAND ----------

# DBTITLE 1,Create the parameters
delta_loc = "dbfs:/FileStore/Users/andrij.demianczuk@databricks.com/tmp/PurchaseOrders/data"
delta_loc_po = "dbfs:/FileStore/Users/andrij.demianczuk@databricks.com/tmp/PurchaseOrders/data_by_po"
write_fmt = 'delta'
table_name = 's_purchaseorders_summary'
write_mode = 'overwrite'
partition_by = "_PurchaseOrderNumber"
database = "default"

# COMMAND ----------

# DBTITLE 1,Write the exploded content to our first silver delta files
# First, let's create our delta file location. We'll be building out Metastore definitions off of this.
df.write \
  .format(write_fmt) \
  .mode(write_mode) \
  .save(delta_loc)

# COMMAND ----------

# DBTITLE 1,**For demos only or if _PurchaseOrderNumber is potentially not unique
spark.sql(f"DROP TABLE IF EXISTS {database}.{table_name}")
spark.sql("CREATE TABLE " + database + "."+ table_name + " USING DELTA LOCATION '" + delta_loc + "'")

# COMMAND ----------

# DBTITLE 1,Create the table (Production - assuming _PurchaseOrderNumber is unique)
# if spark._jsparkSession.catalog().tableExists(database, table_name):

#   print("table already exists..... appending data")
  
#   # Create a view and merge. Since this is a large table with no unique identifiers, this is only here for example.
#   df.createOrReplaceTempView("vw_po_summary")  
  
#   spark.sql(f"MERGE INTO {database}.{table_name} USING vw_po_summary \
#     ON vw_po_summary._PurchaseOrderNumber = {database}.{table_name}._PurchaseOrderNumber \
#     WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
  
# else:
#   print("table does not exist..... creating a new one")
#   # Write the data to its target.
#   spark.sql("CREATE TABLE " + database + "."+ table_name + " USING DELTA LOCATION '" + delta_loc + "'")
