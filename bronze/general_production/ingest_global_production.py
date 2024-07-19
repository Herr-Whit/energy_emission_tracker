# Databricks notebook source
import pyspark
import pyspark.sql.functions as F
import pyspark.sql.types as T




import json
import time
import datetime


# COMMAND ----------

table = 'unity.bronze.general_production'
global_production_reservoir = '/reservoir/general_production'
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

# spark.sql(
#     f"""
#     CREATE TABLE IF NOT EXISTS {table} (
#     meta_data STRING,
#     series VAR,
#     filter_code STRING,
#     region STRING,
#     resolution STRING,
#     request_timestamp STRING,
#     _rescued_data STRING,
#     ingest_time TIMESTAMP
#     )
#     """
# )

# COMMAND ----------

schema = T.StructType([
    T.StructField("meta_data", T.StringType(), True),
    T.StructField("series", T.ArrayType(T.ArrayType(T.StringType())), True),
    T.StructField("_rescued_data", T.StringType(), True)
])

# COMMAND ----------

dbutils.fs.ls(global_production_reservoir)

# COMMAND ----------

stream = spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").schema(schema).load(global_production_reservoir)

# COMMAND ----------

stream = spark.read.schema(schema).json(global_production_reservoir)

# COMMAND ----------

stream = (
    stream
    .withColumn('ingest_time', F.lit(datetime.datetime.now()))
    .withColumn('file_name', F.input_file_name())
    .withColumn('file_name', F.split(F.col('file_name'), '/')[3])
    .withColumn('filter_code', F.split(F.col('file_name'), '_')[0])
    .withColumn('region', F.split(F.col('file_name'), '_')[1])
    .withColumn('resolution', F.split(F.col('file_name'), '_')[2])
    .withColumn('request_timestamp', F.split(F.col('file_name'), '_')[3])
    # .withColumn('request_timestamp', F.col('request_timestamp').cast(T.IntegerType()))
    .drop('file_name')
    )

# COMMAND ----------

display(stream)

# COMMAND ----------

query = (stream.write
         .mode('append')
        #  .option("checkpointLocation", checkpoint_location)
         .saveAsTable(table))

# query.awaitTermination()

# COMMAND ----------

query = (stream.writeStream
         .outputMode('append')
         .option("checkpointLocation", checkpoint_location)
         .toTable(table))

query.awaitTermination()

# COMMAND ----------

display(spark.sql(f'SELECT * FROM {table}'))

# COMMAND ----------


