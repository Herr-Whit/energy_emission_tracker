# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Personal Consumption Data
# MAGIC Stream the requested json files to bronze

# COMMAND ----------

import pyspark
import pyspark.sql.functions as F
import pyspark.sql.types as T

import json
import time
import datetime

# COMMAND ----------

dbutils.widgets.dropdown("processing_mode", "streaming", ["batch", "streaming"])
processing_mode = dbutils.widgets.get("processing_mode")

# COMMAND ----------

bronze_table = "unity.bronze.personal_consumption"
personal_consumption_reservoir = "/reservoir/personal_consumption"
checkpoint_location = "dbfs:/checkpoints/dev_bronze_pc/"

# COMMAND ----------

schema = T.StructType([T.StructField("consumption", T.StringType(), True)])

# COMMAND ----------

if processing_mode == "streaming":
    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(schema)
        .load(personal_consumption_reservoir)
    )
elif processing_mode == "batch":
    stream = spark.read.json(personal_consumption_reservoir, schema)
else:
    raise ValueError("Invalid processing mode")

# COMMAND ----------

stream = stream.withColumn("ingest_time", F.current_timestamp())

# COMMAND ----------

if processing_mode == "batch":
    stream.write.outputMode("append").toTable(bronze_table)
elif processing_mode == "streaming":
    query = (
        stream.writeStream.outputMode("append")
        .option("checkpointLocation", checkpoint_location)
        .toTable(bronze_table)
    )

    query.awaitTermination()
else:
    raise Exception("Invalid processing mode")
