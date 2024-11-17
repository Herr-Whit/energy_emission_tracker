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

dbutils.widgets.dropdown("processing_mode", "streaming", ["batch", "streaming"])
processing_mode = dbutils.widgets.get("processing_mode")

# COMMAND ----------

bronze_table = "unity.bronze.general_production"
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

if processing_mode == "streaming":
    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(schema)
        .load(global_production_reservoir)
    )
elif processing_mode == "batch":
    stream = spark.read.schema(schema).json(global_production_reservoir)
else:
    raise ValueError("Invalid processing mode")

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
