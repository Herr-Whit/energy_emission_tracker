# Databricks notebook source
# MAGIC %md
# MAGIC # Filter
# MAGIC     1223 - Stromerzeugung: Braunkohle
# MAGIC     1224 - Stromerzeugung: Kernenergie
# MAGIC     1225 - Stromerzeugung: Wind Offshore
# MAGIC     1226 - Stromerzeugung: Wasserkraft
# MAGIC     1227 - Stromerzeugung: Sonstige Konventionelle
# MAGIC     1228 - Stromerzeugung: Sonstige Erneuerbare
# MAGIC     4066 - Stromerzeugung: Biomasse
# MAGIC     4067 - Stromerzeugung: Wind Onshore
# MAGIC     4068 - Stromerzeugung: Photovoltaik
# MAGIC     4069 - Stromerzeugung: Steinkohle
# MAGIC     4070 - Stromerzeugung: Pumpspeicher
# MAGIC     4071 - Stromerzeugung: Erdgas
# MAGIC     410 - Stromverbrauch: Gesamt (Netzlast)
# MAGIC     4359 - Stromverbrauch: Residuallast
# MAGIC     4387 - Stromverbrauch: Pumpspeicher
# MAGIC     4169 - Marktpreis: Deutschland/Luxemburg
# MAGIC     5078 - Marktpreis: Anrainer DE/LU
# MAGIC     4996 - Marktpreis: Belgien
# MAGIC     4997 - Marktpreis: Norwegen 2
# MAGIC     4170 - Marktpreis: Österreich
# MAGIC     252 - Marktpreis: Dänemark 1
# MAGIC     253 - Marktpreis: Dänemark 2
# MAGIC     254 - Marktpreis: Frankreich
# MAGIC     255 - Marktpreis: Italien (Nord)
# MAGIC     256 - Marktpreis: Niederlande
# MAGIC     257 - Marktpreis: Polen
# MAGIC     258 - Marktpreis: Polen
# MAGIC     259 - Marktpreis: Schweiz
# MAGIC     260 - Marktpreis: Slowenien
# MAGIC     261 - Marktpreis: Tschechien
# MAGIC     262 - Marktpreis: Ungarn
# MAGIC     3791 - Prognostizierte Erzeugung: Offshore
# MAGIC     123 - Prognostizierte Erzeugung: Onshore
# MAGIC     125 - Prognostizierte Erzeugung: Photovoltaik
# MAGIC     715 - Prognostizierte Erzeugung: Sonstige
# MAGIC     5097 - Prognostizierte Erzeugung: Wind und Photovoltaik
# MAGIC     122 - Prognostizierte Erzeugung: Gesamt
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Region
# MAGIC - DE - Land: Deutschland
# MAGIC - AT - Land: Österreich
# MAGIC - LU - Land: Luxemburg
# MAGIC - DE-LU - Marktgebiet: DE/LU (ab 01.10.2018)
# MAGIC - DE-AT-LU - Marktgebiet: DE/AT/LU (bis 30.09.2018)
# MAGIC - 50Hertz - Regelzone (DE): 50Hertz
# MAGIC - Amprion- Regelzone (DE): Amprion
# MAGIC - TenneT - Regelzone (DE): TenneT
# MAGIC - TransnetBW - Regelzone (DE): TransnetBW
# MAGIC - APG - Regelzone (AT): APG
# MAGIC - Creos - Regelzone (LU): Creos
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Resolution
# MAGIC - hour - Stündlich
# MAGIC - quarterhour - Viertelstündlich
# MAGIC - day - Täglich
# MAGIC - week - Wöchentlich
# MAGIC - month - Monatlich
# MAGIC - year - Jährlich
# MAGIC

# COMMAND ----------

from grid_info import GridInfoClient
import json


# COMMAND ----------

# Define filter region resolution and timestamp as widgets
dbutils.widgets.text('filter', '')
filter_val = dbutils.widgets.get("filter")

dbutils.widgets.dropdown("region", "DE", ["DE", "AT", "LU", "DE-LU", "DE-AT-LU", "50Hertz", "Amprion", "TenneT", "TransNetBW", "APG", "Creos"])
region = dbutils.widgets.get("region")

dbutils.widgets.dropdown("resolution", "hour", ['hour', 'quarterhour', 'day', 'week', 'month', 'year'])
resolution = dbutils.widgets.get("resolution")

dbutils.widgets.text("timestamp", "2022-01-01T00:00:00.000Z")
timestamp = dbutils.widgets.get("timestamp")


# COMMAND ----------

reservoir_path = '/reservoir/general_production/'

# COMMAND ----------

filters = {
    '1223': 'Stromerzeugung: Braunkohle',
    '1224': 'Stromerzeugung: Kernenergie',
    '1225': 'Stromerzeugung: Wind Offshore',
    '1226': 'Stromerzeugung: Wasserkraft',
    '1227': 'Stromerzeugung: Sonstige Konventionelle',
    '1228': 'Stromerzeugung: Sonstige Erneuerbare',
    '4066': 'Stromerzeugung: Biomasse',
    '4067': 'Stromerzeugung: Wind Onshore',
    '4068': 'Stromerzeugung: Photovoltaik',
    '4069': 'Stromerzeugung: Steinkohle',
    '4070': 'Stromerzeugung: Pumpspeicher',
    '4071': 'Stromerzeugung: Erdgas'
}

# COMMAND ----------

client = GridInfoClient()

# COMMAND ----------

filters = list(filters.keys())
for fltr in filters:
    print(f'retrieving data for {fltr}')
    indices = client.get_indices(fltr, region, resolution)
    for i, index in enumerate(indices):
        # print every 10th index
        if i % 10 == 0:
            print(f'processing {i} of {len(indices)} {(fltr, region, resolution, index)=}')
        data = client.get_data(fltr, region, resolution, index)
        json_payload = json.dumps(data)
        file_name = f"{fltr}_{region}_{resolution}_{timestamp}_.json"
        file_path = reservoir_path + file_name
        print(file_path)
        dbutils.fs.put(file_path, json_payload, True)


# COMMAND ----------


