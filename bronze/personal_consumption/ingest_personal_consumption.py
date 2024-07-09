# Databricks notebook source
import pyspark
import pyspark.sql.functions as F
import pyspark.sql.types as T

import json
import time
import datetime


# COMMAND ----------

table = 'unity.bronze.personal_consumption'
personal_consumption_reservoir = '/reservoir/personal_consumption'


# COMMAND ----------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {table} (
    data STRING,
    ingest_time TIMESTAMP
    )
    """
)

# COMMAND ----------

schema = T.StructType([
    T.StructField("data", T.StringType(), True)
])

# COMMAND ----------

dbutils.fs.ls(personal_consumption_reservoir)

# COMMAND ----------

stream = spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").schema(schema).load(personal_consumption_reservoir)

# COMMAND ----------

stream = stream.withColumn("data", F.to_json(F.struct("data"))).withColumn('ingest_time', F.lit(datetime.datetime.now()))

# COMMAND ----------

query = (stream.writeStream
         .outputMode('append') # Adjust the output format as needed
         .option("checkpointLocation", "dbfs:/checkpoints/dev_bronze_pc/")  # Specify checkpoint location
         .trigger(once=True)  # This option ensures the stream triggers once
         .toTable(table))

# Wait for the stream to finish
query.awaitTermination()
