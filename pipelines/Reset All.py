# Databricks notebook source
teardown_nbs = ["pipelines/bronze/teardown_bronze", "pipelines/silver/teardown_silver"]
setup_nbs = ["pipelines/bronze/setup_bronze", "pipelines/silver/setup_silver"]

# COMMAND ----------

for nb in teardown_nbs:
    print(f"Running {nb}")
    dbutils.notebook.run(
        f"/Repos/anton.whittaker@gmail.com/energy_emission_tracker/{nb}", 0
    )

# COMMAND ----------

for nb in setup_nbs:
    print(f"Running {nb}")
    dbutils.notebook.run(
        f"/Repos/anton.whittaker@gmail.com/energy_emission_tracker/{nb}", 0
    )

# COMMAND ----------
