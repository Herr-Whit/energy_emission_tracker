# Databricks notebook source
silver_table = "unity.silver.personal_consumption"
spark.sql(f"DROP TABLE IF EXISTS {silver_table}")

spark.sql(
    """
    DROP TABLE IF EXISTS unity.silver.emissions
    """
)

# COMMAND ----------

try:
    dbutils.fs.rm("/checkpoints/silver/pc_to_silver", True)
except:
    print("checkpoint not found")
