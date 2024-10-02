# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

silver_general_production_table = "unity.silver.general_production"
silver_emissions_table = "unity.silver.emissions"
gold_daily_production = "unity.gold.daily_production"
gold_general_emissions_month = "unity.gold.general_emissions"

# COMMAND ----------

display(spark.read.table(silver_general_production_table))

# COMMAND ----------

display(spark.read.table(silver_emissions_table))

# COMMAND ----------

df = spark.read.table(silver_general_production_table)
# from epoch to datetime
df = df.withColumn("datetime", F.from_unixtime(F.col("timestamp") / 1000))

# COMMAND ----------

df = df.withColumn("year", F.year("datetime"))
df = df.withColumn("month", F.month("datetime"))
df = df.withColumn("day", F.dayofmonth("datetime"))
grouped = df.groupBy("year", "month", "day").sum("production").orderBy("year", "month", "day")

# COMMAND ----------

grouped.createTempView('daily_production')

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE VIEW unity.gold.general_emissions AS SELECT *, g_per_kwh * production as emission_per_source FROM {silver_general_production_table} join (
        SELECT powerplant_id, g_per_kwh FROM {silver_emissions_table}
        WHERE is_active = true
    )  ON powerplant_id = powerplant_id
    """
)

# COMMAND ----------

display(spark.read.table('unity.gold.general_emissions'))

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE VIEW {gold_daily_production} AS SELECT * FROM daily_production
    """
)

# COMMAND ----------

display(spark.read.table(gold_daily_production))
