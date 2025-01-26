# Databricks notebook source
dbutils.widgets.dropdown("mode", "soft", ["soft", "hard"])
mode = dbutils.widgets.get("mode")

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS unity.bronze.general_production")
spark.sql("DROP TABLE IF EXISTS unity.bronze.personal_consumption")
if mode == "hard":
  spark.sql("DROP TABLE IF EXISTS unity.bronze.emissions")

# COMMAND ----------

if mode == "hard":
  spark.sql("DROP DATABASE IF EXISTS unity.bronze")

# COMMAND ----------

to_remove = [
    "/checkpoints/dev_bronze_pc",
    "/checkpoints/dev_bronze_gp",
    "/checkpoints/schema_location/",
]
if mode == "hard":
    to_remove.append("/reservoir")
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
