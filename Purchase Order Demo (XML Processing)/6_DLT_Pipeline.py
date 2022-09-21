# Databricks notebook source
# DBTITLE 1,Imports
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import explode
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Create the initial table from the parsed XML
@dlt.table(
  comment="The silver dataset after it's been parsed by the Purchase-Order-Processing workflow"
)
def dlt_purchaseorders_raw():
  return(spark.table("default.s_PurchaseOrders_summary"))

# COMMAND ----------

# DBTITLE 1,Build the extraction table
@dlt.table(
  comment="The table containing the exploded / flattened Address data"
  #partition_cols = ["State"]
)
def dlt_purchaseorders_addresses():
  df = dlt.read("dlt_purchaseorders_raw")
  df = df.select("*",explode(df.Address))
  df = df.drop(df.Address)
  df = df.select("col.*","*")
  df = df.drop(df.col)
  return df

# COMMAND ----------

# DBTITLE 1,Extract the items details
@dlt.table(
  comment="The table containing the remaining exploded items"
)
def dlt_purchaseorders_details():
  df = dlt.read("dlt_purchaseorders_addresses")
  df = df.select("Items.*","*")
  df = df.drop(df.Items)
  df = df.select("Item.*","*")
  df = df.drop(df.Item)
  return df

# COMMAND ----------

# DBTITLE 1,Summary table: Items sold per PO
@dlt.table(
  comment="Table containing purchase totals by order"
)
def dlt_purchaseorders_pTotals():
  df = dlt.read("dlt_purchaseorders_details")
  df = df.groupBy(col("_PurchaseOrderNumber")).sum(("Price"))
  df = df.withColumnRenamed("sum(Price)","Total_Price")
  return df

# COMMAND ----------

# DBTITLE 1,Summary table: Value per PO
@dlt.table(
  comment="Table containing item counts sold"
)
def dlt_purchaseorders_pQuant():
  df = dlt.read("dlt_purchaseorders_details")
  df = df.groupBy(col("_PurchaseOrderNumber")).sum(("Quantity"))
  df = df.withColumnRenamed("sum(Quantity)","Total_Items_Ordered")
  return df

# COMMAND ----------

# DBTITLE 1,Summary table: Aggregations by product ID
@dlt.table(
  comment="Table with aggregations for each item sold"
)
def dlt_purchaseorders_pAgg():
  df = dlt.read("dlt_purchaseorders_details")
  windowPart = Window.partitionBy("_PartNumber").orderBy("_PartNumber")
  windowPartAgg = Window.partitionBy("_PartNumber")

  df = df.withColumn("row", row_number().over(windowPart)) \
    .withColumn("avg", avg(col("Quantity")).over(windowPartAgg)) \
    .withColumn("sum", sum(col("Quantity")).over(windowPartAgg)) \
    .withColumn("min", min(col("Quantity")).over(windowPartAgg)) \
    .withColumn("max", max(col("Quantity")).over(windowPartAgg)) \
    .withColumn("stddev", stddev(col("Quantity")).over(windowPartAgg)) \
    .where(col("row")==1).select("_PartNumber","ProductName","avg","sum","min","max","stddev")
  
  return df

# COMMAND ----------

# DBTITLE 1,Combine the total price and item count tables for easy reporting
@dlt.table(
  comment="Joined table with purchase totals & item count"
)
def dlt_purchaseorders_pCombined():
  return dlt.read("dlt_purchaseorders_pTotals").join(dlt.read("dlt_purchaseorders_pQuant"), ["_PurchaseOrderNumber"], "inner")
