# Databricks notebook source
to_create = [
    "/reservoir",
    "/reservoir/general_production/",
    "/checkpoints/dev_bronze_pc",
    "/checkpoints/dev_bronze_gp",
    "/checkpoints/schema_location/",
    "/reservoir/emissions_consumption",
]
for location in to_create:
    try:
        dbutils.fs.mkdirs(location)
        print(f"Created {location}")
    except:
        print(f"Failed to create {location}")

# COMMAND ----------

spark.sql(
    """
    CREATE DATABASE IF NOT EXISTS unity.bronze 
    """
)

# COMMAND ----------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS unity.bronze.personal_consumption (
    consumption STRING,
    ingest_time TIMESTAMP
    )
    """
)
spark.sql(
  """
  CREATE TABLE IF NOT EXISTS unity.bronze.general_production (
  meta_data string,
  items ARRAY<STRING>,
  _rescued_data string,
  ingest_time timestamp,
  filter_code string,
  region string,
  resolution string,
  request_timestamp string
  )
  """
)
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS unity.bronze.emissions (
    powerplant_id STRING,
    powerplant_name STRING,
    g_per_kwh DOUBLE
    )
    """
)

# COMMAND ----------

display(dbutils.fs.ls("/"))
try:
    display(dbutils.fs.ls("/reservoir"))
except:
    pass
