# Databricks notebook source
# DBTITLE 1,Imports
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

import dlt, json, sys, os, time

# COMMAND ----------

# MAGIC %md
# MAGIC ## Variables & Scope
# MAGIC Below we're doing a few things:
# MAGIC 1. First, we're relying on the dlt workflow config to store our variables that might differ between orgs.
# MAGIC 2. Second, we're storing a handful of our variables in the databricks Secrets store. This is valuable because it's a safe, encrypted store of variables that never need to be exposed.
# MAGIC 3. Finally, we're hard-coding our schema. This is *okay* as long as it doesn't change, but ideally we'd want to evaluate this against the unknown structure of the payload. If the structure changes, we risk losing data by hard-coding it this way however for a POC this will serve our needs just fine.

# COMMAND ----------

# DBTITLE 1,Declarations & Init
#variable init
scope = spark.conf.get("secrets_scope")
project_id = spark.conf.get("project_id")
table_name = spark.conf.get("table_name")
topic = spark.conf.get("topic")
sub = spark.conf.get("subscription")

#Create our authorization struct
authOptions = {"clientID": dbutils.secrets.get(scope=scope, key="clientId"),
                "clientEmail": dbutils.secrets.get(scope=scope, key="clientEmail"),
                "privateKey": dbutils.secrets.get(scope=scope, key="privateKey"),
                "privateKeyId": dbutils.secrets.get(scope=scope, key="privateKeyId")}

#Payload Structure
schema = """
    STRUCT<
        subscriber_id: STRING,
        userequipment_manufacturer_name: STRING,
        userequipment_model_name: STRING,
        application_protocol_type_code: STRING,
        application_name: STRING,
        application_group: STRING,
        message_protocol_type_code: STRING,
        message_name: STRING,
        rat_name: STRING,
        response_code: STRING,
        response_description: STRING,
        controlplane_transaction_status_name: STRING,
        visited_plmn_name: STRING,
        home_plmn_name: STRING,
        cell_area: INT,
        cell_id: INT,
        cell_mcc: INT,
        cell_mnc: INT,
        cell_updated_flag: INT,
        cell_region_name: STRING,
        cell_subdivision_name: STRING,
        cell_city_name: STRING,
        celltype_name: STRING,
        trackingarea_code: INT,
        apn_name: STRING,
        origin_host_diameter_name: STRING,
        destination_host_diameter_name: STRING,
        origin_realm_diameter_name: STRING,
        destination_realm_diameter_name: STRING,
        speech_indicator: INT,
        csfb_indicator_name: STRING,
        client_server_entry_type_name: STRING,
        controlplane_response_time_usec: FLOAT,
        cal_timestamp_time: TIMESTAMP,
        device_name: STRING,
        subscriber_msisdn: STRING,
        subscriber_company_name: STRING,
        subscriber_segment_name: STRING,
        subscriber_plan_name: STRING,
        userequipment_imeisv: STRING,
        userequipment_sv: STRING,
        userequipment_tac: STRING,
        calling_address: STRING,
        called_address: STRING,
        controlplane_transaction_start_time: DATE,
        controlplane_transaction_id: DATE,
        cell_latitude: STRING,
        cell_longitude: STRING,
        create_ts: DATE,
        create_user_id: STRING,
        create_updt_ts: DATE,
        create_updt_user_id: STRING
    >
"""

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
    .withColumn("eventTime", current_timestamp())
    )

@dlt.table(table_properties={"pipelines.reset.allowed":"true"})
def pubsub_bronze():
  return raw_pubsub_events

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parsing the Stream Events
# MAGIC Parsing the stream events is a fairly straightforward process of decoding the binary column with the schema defined above. We want to preserve the messageId field and timestamp the event as it's being processed to keep track of what's going on and which records are related. The messageId is how we'll stictch together the timeline later on.
# MAGIC * The messageId is the unique identifier for the event
# MAGIC * The payload is the encoded stream values
# MAGIC * The publishTimestampInMillis is when the original generator of the event triggered
# MAGIC * eventTime is when the table receives the event

# COMMAND ----------

# DBTITLE 1,Decode Bronze to Struct
# @dlt.table(comment="The parsed table. This table contains the decoded JSON content.")

@dlt.table(comment="Parsed DTS data from the JSON sample file", table_properties={"pipelines.reset.allowed": "true"})
def pubsub_bronze_decoded():

  #Parse the stream
  read_stream = dlt.readStream("pubsub_bronze").select(col("messageId").cast("string").alias("eventId"), from_json(col("payload").cast("string"), schema).alias("json"), col("publishTimestampInMillis")).select("json.*", "eventId", "publishTimestampInMillis").withColumn("eventTime", current_timestamp())

  #Create the new live table
  return read_stream

# COMMAND ----------

# MAGIC %md
# MAGIC ## Temporary Views in DLT
# MAGIC DLT supports the concept of both temporary tables and temporary views. This is handy in our circumstance because we will only be joining on a subset of the data. **Technically** we can accomplish the same thing **if** we know the partition structure of the DNTL table - if this where the case and we know for example that the partition column is set up on bus_object_typ_cd then we can get away without the view and instead rely on the predicate clause instead. Using a view however does make things a bit more readable so it may be preferable if this logic is passed around for future growth.

# COMMAND ----------

# DBTITLE 1,Create the DNTL View
@dlt.view(
    name="dntl_vw",
    comment="This view only contains a subset of the dntl data that we require for a left-static join",
)
def dntl_vw():

    #Replace the following name with the location of the current dntl table. This might be different in prod.
    return spark.table("networks_poc.netscout.dntl").filter(
        col("bus_object_typ_cd") == "IMSI"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static-stream Joins
# MAGIC Databricks supports joining static tables to streaming data, but requires that the static tables be on the left of the join. This is handy when we need to enrich our data with lookup tables.

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# DBTITLE 1,Decoded Bronze to Silver
@dlt.table(
    comment="Parsed DTS data from the JSON sample file",
    table_properties={"pipelines.reset.allowed": "true"},
)
def pubsub_silver():
    sdf = dlt.readStream("pubsub_bronze_decoded").withColumn("eventTime", current_timestamp())
    dntl_df = dlt.read("dntl_vw")
    return sdf.join(dntl_df, sdf.subscriber_id == dntl_df.bus_object_id, "left_semi")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Window Functions
# MAGIC Window functions are a useful tool that we can use to 'bucket' events into aggregations. For this example, we will be doing some average values for certain time groups based on the timestamp values
# MAGIC
# MAGIC ### Stateful Processing
# MAGIC More info on watermarks and stateful processing can be [located here](https://docs.databricks.com/en/delta-live-tables/stateful-processing.html)

# COMMAND ----------

# DBTITLE 1,Silver to Gold
@dlt.table(comment="Aggregated values by window", table_properties={"pipelines.reset.allowed": "true"})
def pubsub_gold():
  
  df = dlt.readStream("pubsub_silver")
  
  df = (df.groupBy(window("eventTime", "60 minutes", "10 minutes"))
        .agg(approx_count_distinct("userequipment_model_name").alias("count_dist_device_model"))
        .orderBy(col("window.start")))
  return df

# COMMAND ----------

# @dlt.table(comment="Message Counts per window", table_properties={"pipelines.reset.allowed": "true"})
# def pubsub_message_status_gold():
  
#   df = dlt.readStream("pubsub_silver")
  
#   df = (df.groupBy("well", "segment", window("eventTime", "60 minutes", "10 minutes"))
#         .agg(countDistinct("userequipment_model_name").alias("count_dist_device_model"), countDistinct("cell_area").alias("count_dist_area"), countDistinct("create_user_id").alias("count_dist_users"))
#         .orderBy(col("window.start")))
#   return df
