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

display(dbutils.fs.ls("/"))
try:
    display(dbutils.fs.ls("/reservoir"))
except:
    pass
