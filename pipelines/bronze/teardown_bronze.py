# Databricks notebook source
spark.sql("DROP TABLE IF EXISTS unity.bronze.general_production")
spark.sql("DROP TABLE IF EXISTS unity.bronze.personal_consumption")
spark.sql("DROP TABLE IF EXISTS unity.bronze.emissions")

# COMMAND ----------

spark.sql("DROP DATABASE IF EXISTS unity.bronze")

# COMMAND ----------

to_remove = [
    "/reservoir",
    "/checkpoints/dev_bronze_pc",
    "/checkpoints/dev_bronze_gp",
    "/checkpoints/schema_location/",
]
for location in to_remove:
    try:
        dbutils.fs.rm(location, recurse=True)
        print(f"Removed {location}")
    except:
        print(f"Failed to remove {location}")

# COMMAND ----------

display(dbutils.fs.ls("/"))
try:
    display(dbutils.fs.ls("/reservoir"))
except:
    pass
