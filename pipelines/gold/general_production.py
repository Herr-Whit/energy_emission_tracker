# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

silver_general_production_table = "unity.silver.general_production"
gold_general_production_month = "unity.gold.general_production_monthly"
gold_general_production = "unity.gold.general_production"

# COMMAND ----------

display(spark.read.table(silver_general_production_table))

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE VIEW {gold_general_production_month} AS SELECT month, sum(cost) as cost, sum(consumption) as production FROM {silver_general_production_table} GROUP BY month
    """
)

# COMMAND ----------

display(spark.read.table(gold_general_production_month))

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE VIEW {gold_general_production} AS SELECT * FROM {silver_general_production_table}
    """
)

# COMMAND ----------

df = spark.read.table(gold_general_production)
display(df)

# COMMAND ----------

display(df.groupBy("month").agg({"consumption": "sum", "cost": "sum"}))

# COMMAND ----------
