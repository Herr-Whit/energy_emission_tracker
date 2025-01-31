# Databricks notebook source
spark.sql(f"DROP TABLE IF EXISTS unity.silver.personal_consumption")
spark.sql(f"DROP TABLE IF EXISTS unity.silver.general_production")
spark.sql("DROP TABLE IF EXISTS unity.silver.emissions")

# COMMAND ----------

spark.sql("DROP DATABASE IF EXISTS unity.silver")

# COMMAND ----------

try:
    dbutils.fs.rm("/checkpoints/silver/pc_to_silver", True)
    dbutils.fs.rm("/checkpoints/silver/gp_to_silver", True)
    dbutils.fs.rm("checkpoints/dev_bronze_emissions/", True)
except:
    print("checkpoint not found")
