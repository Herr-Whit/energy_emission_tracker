# Databricks notebook source
import pyspark
import pyspark.sql.functions as F
import pyspark.sql.types as T




import json
import time
import datetime


# COMMAND ----------

table = 'unity.bronze.global_production'
global_production_reservoir = '/reservoir/global_production'
checkpoint_location = "dbfs:/checkpoints/dev_bronze_gp/" 
schema_location = "dbfs:/checkpoints/schema_location/" 


# COMMAND ----------

if True:
    spark.sql(
    f"""
    DROP TABLE {table}
    """
    )
    dbutils.fs.rm(checkpoint_location, True)

# COMMAND ----------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {table} (
    meta_data STRING,
    series STRING,
    filter_code STRING,
    region STRING,
    resolution STRING,
    request_timestamp INT,
    _rescued_data STRING,
    ingest_time TIMESTAMP
    )
    """
)

# COMMAND ----------

schema = T.StructType([
    T.StructField("meta_data", T.StringType(), True),
    T.StructField("series", T.StringType(), True),
    T.StructField("_rescued_data", T.StringType(), True)
])

# COMMAND ----------

dbutils.fs.ls(global_production_reservoir)

# COMMAND ----------

stream = spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").schema(schema).load(global_production_reservoir)

# COMMAND ----------

stream = stream.withColumn('ingest_time', F.lit(datetime.datetime.now()))

# COMMAND ----------

query = (stream.writeStream

         .outputMode('append')
         .option("checkpointLocation", checkpoint_location)
         .toTable(table))

query.awaitTermination()

# COMMAND ----------

display(spark.sql(f'SELECT * FROM {table}'))

# COMMAND ----------


