# Databricks notebook source
import pandas as pd


files = [
{"file":"map_cities"},
{"file":"map_cancellation_reasons"},
{"file":"bulk_rides"},
{"file":"map_payment_methods"},
{"file":"map_ride_statuses"},
{"file":"map_vehicle_makes"},
{"file":"map_vehicle_types"}
]


for file in files:
  url = f"https://nyashaindrivestorage.blob.core.windows.net/raw/{file['file']}.json?sp=r&st=2026-04-20T11:29:39Z&se=2026-04-20T19:44:39Z&spr=https&sv=2025-11-05&sr=c&sig=IsoHFhATiTHSQTupzp4FCF4WsVnN4xK1XwDqJwHOXfc%3D"

  df = pd.read_json(url)
  df_spark = spark.createDataFrame(df)


  #Writing data to the bronze layer
  df_spark.write.format("delta")\
                .mode("overwrite")\
                .option("overwriteSchema", "true")\
                .saveAsTable(f"indrive_catalog.indrive_schema.{file['file']}")
                              






# COMMAND ----------

# MAGIC %sql
# MAGIC select * from indrive_catalog.indrive_schema.map_cities
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from indrive_catalog.indrive_schema.rides_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from indrive_catalog.indrive_schema.dim_passenger
