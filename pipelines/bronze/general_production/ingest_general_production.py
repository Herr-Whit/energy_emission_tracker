# Databricks notebook source
# MAGIC %md
# MAGIC Ingest a versioned record of general production data to bronze

# COMMAND ----------

import pyspark
import pyspark.sql.functions as F
import pyspark.sql.types as T

import json
import time
import datetime


# COMMAND ----------

table = "unity.bronze.general_production"
global_production_reservoir = "/reservoir/general_production"
checkpoint_location = "dbfs:/checkpoints/dev_bronze_gp/"
schema_location = "dbfs:/checkpoints/schema_location/"


# COMMAND ----------

schema = T.StructType(
    [
        T.StructField("meta_data", T.StringType(), True),
        T.StructField("series", T.ArrayType(T.ArrayType(T.StringType())), True),
        T.StructField("_rescued_data", T.StringType(), True),
    ]
)

# COMMAND ----------

stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .schema(schema)
    .load(global_production_reservoir)
)

# COMMAND ----------

stream = (
    stream.withColumn("ingest_time", F.current_timestamp())
    .withColumn("file_name", F.input_file_name())
    .withColumn("file_name", F.split(F.col("file_name"), "/")[3])
    .withColumn("filter_code", F.split(F.col("file_name"), "_")[0])
    .withColumn("region", F.split(F.col("file_name"), "_")[1])
    .withColumn("resolution", F.split(F.col("file_name"), "_")[2])
    .withColumn("request_timestamp", F.split(F.col("file_name"), "_")[3])
    .drop("file_name")
)

# COMMAND ----------

query = (
    stream.writeStream.outputMode("append")
    .option("checkpointLocation", checkpoint_location)
    .toTable(table)
)

query.awaitTermination()

# COMMAND ----------
