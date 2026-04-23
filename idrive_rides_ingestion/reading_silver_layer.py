# Databricks notebook source
# MAGIC %md
# MAGIC # Stream Rides Transformation

# COMMAND ----------


from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.table("indrive_catalog.indrive_schema.rides_raw")
display(df)

# COMMAND ----------

# Copyoing the schema from the bulk dataset

df_schema = spark.sql("SELECT * FROM indrive_catalog.indrive_schema.bulk_rides")
df_schema.schema

rides_raw_schema = StructType([StructField('ride_id', StringType(), True), StructField('confirmation_number', StringType(), True), StructField('passenger_id', StringType(), True), StructField('driver_id', StringType(), True), StructField('vehicle_id', StringType(), True), StructField('pickup_location_id', StringType(), True), StructField('dropoff_location_id', StringType(), True), StructField('vehicle_type_id', LongType(), True), StructField('vehicle_make_id', LongType(), True), StructField('payment_method_id', LongType(), True), StructField('ride_status_id', LongType(), True), StructField('pickup_city_id', LongType(), True), StructField('dropoff_city_id', LongType(), True), StructField('cancellation_reason_id', LongType(), True), StructField('passenger_name', StringType(), True), StructField('passenger_email', StringType(), True), StructField('passenger_phone', StringType(), True), StructField('driver_name', StringType(), True), StructField('driver_rating', DoubleType(), True), StructField('driver_phone', StringType(), True), StructField('driver_license', StringType(), True), StructField('vehicle_model', StringType(), True), StructField('vehicle_color', StringType(), True), StructField('license_plate', StringType(), True), StructField('pickup_address', StringType(), True), StructField('pickup_latitude', DoubleType(), True), StructField('pickup_longitude', DoubleType(), True), StructField('dropoff_address', StringType(), True), StructField('dropoff_latitude', DoubleType(), True), StructField('dropoff_longitude', DoubleType(), True), StructField('distance_miles', DoubleType(), True), StructField('duration_minutes', LongType(), True), StructField('booking_timestamp', TimestampType(), True), StructField('pickup_timestamp', StringType(), True), StructField('dropoff_timestamp', StringType(), True), StructField('base_fare', DoubleType(), True), StructField('distance_fare', DoubleType(), True), StructField('time_fare', DoubleType(), True), StructField('surge_multiplier', DoubleType(), True), StructField('subtotal', DoubleType(), True), StructField('tip_amount', DoubleType(), True), StructField('total_fare', DoubleType(), True), StructField('rating', DoubleType(), True)])


# COMMAND ----------

# Save the schema to a variable


df_parsed = df.withColumn("parsed_rides", from_json(col("rides"), rides_raw_schema))
df_parsed = df_parsed.select("parsed_rides.*")
              
display(df_parsed)




# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM indrive_catalog.indrive_schema.rides_strm_table

# COMMAND ----------

# MAGIC %md
# MAGIC #Jinja Template For OBT
# MAGIC

# COMMAND ----------

pip install jinja2

# COMMAND ----------

# DBTITLE 1,Cell 9
jinja_config = [
    {
        "table" : "indrive_catalog.indrive_schema.rides_strm_table ride_strm_table",
        "select" : "ride_strm_table.ride_id, ride_strm_table.confirmation_number, ride_strm_table.passenger_id, ride_strm_table.driver_id, ride_strm_table.vehicle_id, ride_strm_table.pickup_location_id, ride_strm_table.dropoff_location_id, ride_strm_table.vehicle_type_id, ride_strm_table.vehicle_make_id, ride_strm_table.payment_method_id, ride_strm_table.ride_status_id, ride_strm_table.pickup_city_id, ride_strm_table.dropoff_city_id, ride_strm_table.cancellation_reason_id, ride_strm_table.passenger_name, ride_strm_table.passenger_email, ride_strm_table.passenger_phone, ride_strm_table.driver_name,ride_strm_table.driver_rating, ride_strm_table.driver_phone, ride_strm_table.driver_license, ride_strm_table.vehicle_model, ride_strm_table.vehicle_color, ride_strm_table.license_plate, ride_strm_table.pickup_address, ride_strm_table.pickup_latitude, ride_strm_table.pickup_longitude, ride_strm_table.dropoff_address, ride_strm_table.dropoff_latitude, ride_strm_table.dropoff_longitude, ride_strm_table.distance_miles, ride_strm_table.duration_minutes, ride_strm_table.booking_timestamp, ride_strm_table.pickup_timestamp, ride_strm_table.dropoff_timestamp, ride_strm_table.base_fare, ride_strm_table.distance_fare, ride_strm_table.time_fare, ride_strm_table.surge_multiplier, ride_strm_table.subtotal, ride_strm_table.tip_amount, ride_strm_table.total_fare, ride_strm_table.rating",
        "where" : ""

    },
    {
        "table" : "indrive_catalog.indrive_schema.map_cities map_cities",
        "select" : "map_cities.city as pickup_city, map_cities.state, map_cities.region",
        "where" : "",
        "on":"ride_strm_table.pickup_city_id = map_cities.city_id"
    },
    
    {
        "table" : "indrive_catalog.indrive_schema.map_cancellation_reasons map_cancellation_reasons",
        "select" : "map_cancellation_reasons.cancellation_reason",
        "where" : "",
        "on":"ride_strm_table.cancellation_reason_id = map_cancellation_reasons.cancellation_reason_id"
    },
    {
        "table" : "indrive_catalog.indrive_schema.map_payment_methods map_payment_methods",
        "select" : "map_payment_methods.payment_method, map_payment_methods.is_card, map_payment_methods.requires_auth",
        "where" : "",
        "on":"ride_strm_table.payment_method_id = map_payment_methods.payment_method_id"
    },
    {
        "table" : "indrive_catalog.indrive_schema.map_ride_statuses map_ride_statuses",
        "select" : "map_ride_statuses.ride_status, map_ride_statuses.is_completed",
        "where" : "",
        "on":"ride_strm_table.ride_status_id = map_ride_statuses.ride_status_id"
    },
    {
        "table" : "indrive_catalog.indrive_schema.map_vehicle_makes map_vehicle_makes",
        "select" : "map_vehicle_makes.vehicle_make",
        "where" : "",
        "on":"ride_strm_table.vehicle_make_id = map_vehicle_makes.vehicle_make_id"
    },
    {
        "table" : "indrive_catalog.indrive_schema.map_vehicle_types map_vehicle_types",
        "select" : "map_vehicle_types.vehicle_type, map_vehicle_types.vehicle_type, map_vehicle_types.description, map_vehicle_types.base_rate, map_vehicle_types.per_mile, map_vehicle_types.per_minute",
        "where" : "",
        "on":"ride_strm_table.vehicle_type_id = map_vehicle_types.vehicle_type_id"
            
    }
]

# COMMAND ----------

from jinja2 import Template

jinja_string = """
 
SELECT
    {% for config in jinja_config %}
        {{ config.select }} 
            {% if not loop.last %}
                , 
            {% endif %}
    {% endfor %}
From
    {% for config in jinja_config %}
        {% if loop.first %}
           {{config.table}}
        {% else %}
           left join {{config.table}} ON {{config.on}}
        {% endif %}
    {% endfor %}

    {% for config in jinja_config %}

         {% if loop.last %}
             {% if config.where != "" %}
               WHERE
             {% endif %}
         {% endif %}

        {{ config.where }} 
            {% if not loop.last %}
               {% if config.where != "" %}
                 AND 
               {% endif %}
            {% endif %}
    {% endfor %}

"""

jinja_template = Template(jinja_string)
rendered_template = jinja_template.render(jinja_config=jinja_config)
print(rendered_template)

# COMMAND ----------

spark.sql(rendered_template).display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from indrive_catalog.indrive_schema.silver_one_big_table

# COMMAND ----------

# MAGIC %sql
# MAGIC describe indrive_catalog.indrive_schema.silver_one_big_table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from indrive_catalog.indrive_schema.fact_rides

# COMMAND ----------

# MAGIC %sql
# MAGIC select fact.ride_id, dim_p.passenger_name,dim_d.driver_name, div_v.vehicle_model, dim_p.pickup_address from indrive_catalog.indrive_schema.fact_rides as fact
# MAGIC left join indrive_catalog.indrive_schema.dim_passenger as dim_p
# MAGIC on fact.passenger_id = dim_p.passenger_id
# MAGIC left join indrive_catalog.indrive_schema.dim_driver as dim_d
# MAGIC on fact.driver_id = dim_d.driver_id
# MAGIC left join indrive_catalog.indrive_schema.dim_vehicle as div_v
# MAGIC on fact.vehicle_id = div_v.vehicle_id
# MAGIC left join indrive_catalog.indrive_schema.dim_pickup_location as dim_p
# MAGIC on fact.pickup_location_id = dim_p.pickup_location_id
# MAGIC
# MAGIC
