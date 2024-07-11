# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

silver_personal_consumption_table = 'unity.silver.personal_consumption'
gold_personal_consumption_month = 'unity.gold.personal_consumption_monthly'
gold_personal_consumption = 'unity.gold.personal_consumption'

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE VIEW {gold_personal_consumption_month} AS SELECT month, sum(cost) as cost, sum(consumption) as consumption FROM {silver_personal_consumption_table} GROUP BY month
    """
)

# COMMAND ----------

display(spark.read.table(gold_personal_consumption_month))

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE VIEW {gold_personal_consumption} AS SELECT * FROM {silver_personal_consumption_table}
    """
)

# COMMAND ----------

df= (spark.read.table(gold_personal_consumption))
display(df)

# COMMAND ----------

display(df.groupBy('month').agg({'consumption': 'sum', 'cost': 'sum'}))

# COMMAND ----------


