# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://www.cdata.com/ui/img/logo-googlecloudstorage.png" />
# MAGIC 
# MAGIC ## Introduction
# MAGIC In this series, we will be connecting to a GCS bucket and loading some XML data in systematically for some basic ETL. This is a common use-case and although XML isn't as widely used as other storage formats it is still considered powerful with it's recursion and nesting capabilities.
# MAGIC 
# MAGIC [Getting Started With GCS on Databricks](https://docs.gcp.databricks.com/data/data-sources/google/gcs.html)
# MAGIC 
# MAGIC ## Part 3: Extracting Addresses
# MAGIC Now that we have our dataset exploded into one PO per row, we can start extracting the remaining structs and arrays. In this notebook we will extract the shipping & billing information as well as item details.

# COMMAND ----------

df = spark.table("default.s_PurchaseOrders_summary")
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Explode the PurchaseOrder array (to get one row for each order)
#Our first nested structure is an array so we'll explode each PurchaseOrder out to it's own row with the explode() function
from pyspark.sql.functions import explode
df = df.select("*",explode(df.Address))
df = df.drop(df.Address)
df = df.select("col.*","*")
df = df.drop(df.col)
df.printSchema()

# COMMAND ----------

df = df.select("Items.*","*")
df = df.drop(df.Items)
df = df.select("Item.*","*")
df = df.drop(df.Item)
display(df)

# COMMAND ----------

# DBTITLE 1,Create the parameters
delta_loc = "dbfs:/FileStore/Users/andrij.demianczuk@databricks.com/tmp/PurchaseOrders/data"
delta_loc_detail = "dbfs:/FileStore/Users/andrij.demianczuk@databricks.com/tmp/PurchaseOrders/detail"
write_fmt = 'delta'
table_name = 's_purchaseorders_detail'
write_mode = 'overwrite'
partition_by = "State"
database = "default"

# COMMAND ----------

# DBTITLE 1,Write the exploded content to our first silver delta files
# First, let's create our delta file location. We'll be building out Metastore definitions off of this.
df.write \
  .format(write_fmt) \
  .mode(write_mode) \
  .partitionBy(partition_by) \
  .save(delta_loc_detail)

# COMMAND ----------

# DBTITLE 1,**For demos only or if _PurchaseOrderNumber is potentially not unique
spark.sql(f"DROP TABLE IF EXISTS {database}.{table_name}")
spark.sql("CREATE TABLE " + database + "."+ table_name + " USING DELTA LOCATION '" + delta_loc_detail + "'")

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
