# Databricks notebook source
# DBTITLE 1,Imports
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import explode

# COMMAND ----------

# DBTITLE 1,Create the initial table from the parsed XML
@dlt.table(
  comment="The silver dataset after it's been parsed by the Purchase-Order-Processing workflow"
)
def dlt_purchaseorders_raw():
  return(spark.table("ademianczuk.s_PurchaseOrders_summary"))

# COMMAND ----------

# DBTITLE 1,Build the extraction table
@dlt.table(
  comment="The table containing the exploded / flattened data"
  #partition_cols = [""]
)
def dlt_purchaseorders_details():
  df = dlt.read("dlt_purchaseorders_raw")
  df = df.select("*",explode(df.Address))
  df = df.drop(df.Address)
  df = df.select("col.*","*")
  df = df.drop(df.col)
  return df
