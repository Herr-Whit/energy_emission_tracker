# Databricks notebook source
spark.sql(f"DROP TABLE IF EXISTS unity.silver.personal_consumption")
spark.sql(f"DROP TABLE IF EXISTS unity.silver.general_production")
spark.sql("DROP TABLE IF EXISTS unity.silver.emissions")

# COMMAND ----------

spark.sql("DROP DATABASE IF EXISTS unity.silver")

# COMMAND ----------

try:
    dbutils.fs.rm("/checkpoints/silver/pc_to_silver", True)
except:
    print("checkpoint not found")
