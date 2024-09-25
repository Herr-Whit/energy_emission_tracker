# Databricks notebook source
spark.sql(
    """
    CREATE DATABASE IF NOT EXISTS unity.silver
    """
)

# COMMAND ----------

silver_table = 'unity.silver.personal_consumption'
spark.sql(f"DROP TABLE IF EXISTS {silver_table}")

# COMMAND ----------


