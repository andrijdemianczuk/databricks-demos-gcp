# Databricks notebook source
# DBTITLE 1,Declarations & Init
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

#variable init
scope = "telus-poc"
project_id = "fe-dev-sandbox"
table_name = "control_plane"
topic = f"telus_poc_control_plane"
sub = f"ad-sub1-telus-poc"

#Create our authorization struct
authOptions = {"clientID": dbutils.secrets.get(scope=scope, key="clientId"),
                "clientEmail": dbutils.secrets.get(scope=scope, key="clientEmail"),
                "privateKey": dbutils.secrets.get(scope=scope, key="privateKey"),
                "privateKeyId": dbutils.secrets.get(scope=scope, key="privateKeyId")}

# COMMAND ----------

# DBTITLE 1,Stream Source to Bronze
# This setting makes PARTION BY writes faster, but could make reads slower.
# We generally only recommend doing this when most of the data falls into the same partition
# like in this case because we're partitioning everything into today's date.

# read pubsub data
raw_pubsub_events =  (spark.readStream.format("pubsub")
    .option("subscriptionId", sub)
    .option("topicId", topic)
    .option("projectId", project_id)
    .options(**authOptions)
    # .option("numFetchPartitions", 50)   
    .load()
    )

@dlt.table(table_properties={"pipelines.reset.allowed":"false"})
def pubsub_bronze():
  return raw_pubsub_events

# COMMAND ----------

# DBTITLE 1,Bronze to Silver


# COMMAND ----------

# DBTITLE 1,Silver to Gold


# COMMAND ----------

# DBTITLE 1,Gold to BigQuery

