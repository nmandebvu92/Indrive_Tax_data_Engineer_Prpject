from pyspark import pipelines as dp

#Passenger dim table.
  
#Create a view first to store the data from the selected columns.

@dp.view
def dim_passenger_view():
  df = spark.readStream.table("silver_one_big_table")
  df = df.select("passenger_id", "passenger_name", "passenger_email", "passenger_phone")
  df = df.dropDuplicates(subset = ["passenger_id"])
  return df

#Create streaming table

dp.create_streaming_table("dim_passenger")
dp.create_auto_cdc_flow(
  target = "dim_passenger",
  source = "dim_passenger_view",
  keys = ["passenger_id"],
  sequence_by = "passenger_id",
  stored_as_scd_type = 1, 
)

#Dim table driver

@dp.view
def dim_driver_view():
  df = spark.readStream.table("silver_one_big_table")
  df = df.select("driver_id", "driver_name", "driver_rating", "driver_phone", "driver_license")
  df = df.dropDuplicates(subset = ["driver_id"])
  return df

#Create streaming table

dp.create_streaming_table("dim_driver")
dp.create_auto_cdc_flow(
  target = "dim_driver",
  source = "dim_driver_view",
  keys = ["driver_id"],
  sequence_by = "driver_id",
  stored_as_scd_type = 1,
)

#Dim table vehicle

@dp.view
def dim_vehicle_view():
  df = spark.readStream.table("silver_one_big_table")
  df = df.select("vehicle_id", "vehicle_model", "vehicle_color", "license_plate", "vehicle_type_id", "vehicle_make_id")
  df = df.dropDuplicates(subset = ["vehicle_id"])
  return df

#Create streaming table

dp.create_streaming_table("dim_vehicle")
dp.create_auto_cdc_flow(
  target = "dim_vehicle",
  source = "dim_vehicle_view",
  keys = ["vehicle_id"],
  sequence_by = "vehicle_id",
  stored_as_scd_type = 1,
)

#Dim table pickup_location

@dp.view
def dim_pickup_location_view():
  df = spark.readStream.table("silver_one_big_table")
  df = df.select("pickup_location_id", "pickup_address", "pickup_latitude", "pickup_longitude", "pickup_city_id")
  df = df.dropDuplicates(subset = ["pickup_location_id"])
  return df

#Create streaming table

dp.create_streaming_table("dim_pickup_location")
dp.create_auto_cdc_flow(
  target = "dim_pickup_location",
  source = "dim_pickup_location_view",
  keys = ["pickup_location_id"],
  sequence_by = "pickup_location_id",
  stored_as_scd_type = 1,
)

#Dim table dropoff_location

@dp.view
def dim_dropoff_location_view():
  df = spark.readStream.table("silver_one_big_table")
  df = df.select("dropoff_location_id", "dropoff_address", "dropoff_latitude", "dropoff_longitude", "dropoff_city_id")
  df = df.dropDuplicates(subset = ["dropoff_location_id"])
  return df

#Create streaming table

dp.create_streaming_table("dim_dropoff_location")
dp.create_auto_cdc_flow(
  target = "dim_dropoff_location",
  source = "dim_dropoff_location_view",
  keys = ["dropoff_location_id"],
  sequence_by = "dropoff_location_id",
  stored_as_scd_type = 1,
)

#Dim table pickup_city

@dp.view
def dim_pickup_city_view():
  df = spark.readStream.table("silver_one_big_table")
  df = df.select("pickup_city_id", "pickup_city", "state", "region")
  df = df.dropDuplicates(subset = ["pickup_city_id"])
  return df

#Create streaming table

dp.create_streaming_table("dim_pickup_city")
dp.create_auto_cdc_flow(
  target = "dim_pickup_city",
  source = "dim_pickup_city_view",
  keys = ["pickup_city_id"],
  sequence_by = "pickup_city_id",
  stored_as_scd_type = 1,
)

#Dim table cancellation_reason

@dp.view
def dim_cancellation_reason_view():
  df = spark.readStream.table("silver_one_big_table")
  df = df.select("cancellation_reason_id", "cancellation_reason")
  df = df.dropDuplicates(subset = ["cancellation_reason_id"])
  return df

#Create streaming table

dp.create_streaming_table("dim_cancellation_reason")
dp.create_auto_cdc_flow(
  target = "dim_cancellation_reason",
  source = "dim_cancellation_reason_view",
  keys = ["cancellation_reason_id"],
  sequence_by = "cancellation_reason_id",
  stored_as_scd_type = 1,
)

#Dim table payment_method

@dp.view
def dim_payment_method_view():
  df = spark.readStream.table("silver_one_big_table")
  df = df.select("payment_method_id", "payment_method", "is_card", "requires_auth")
  df = df.dropDuplicates(subset = ["payment_method_id"])
  return df

#Create streaming table

dp.create_streaming_table("dim_payment_method")
dp.create_auto_cdc_flow(
  target = "dim_payment_method",
  source = "dim_payment_method_view",
  keys = ["payment_method_id"],
  sequence_by = "payment_method_id",
  stored_as_scd_type = 1,
)

#Dim vehicle type

@dp.view
def dim_vehicle_type_view():
  df = spark.readStream.table("silver_one_big_table")
  df = df.select("vehicle_type_id", "vehicle_type", "description", "base_rate","per_mile", "per_minute")
  df = df.dropDuplicates(subset = ["vehicle_type_id"])
  return df

#Create streaming table

dp.create_streaming_table("dim_vehicle_type")
dp.create_auto_cdc_flow(
  target = "dim_vehicle_type",
  source = "dim_vehicle_type_view",
  keys = ["vehicle_type_id"],
  sequence_by = "vehicle_type_id",
  stored_as_scd_type = 1,
)

#Dim vehicle make

@dp.view
def dim_vehicle_make_view():
  df = spark.readStream.table("silver_one_big_table")
  df = df.select("vehicle_make_id", "vehicle_make", "description")
  df = df.dropDuplicates(subset = ["vehicle_make_id"])
  return df

#Create streaming table

dp.create_streaming_table("dim_vehicle_make")
dp.create_auto_cdc_flow(
  target = "dim_vehicle_make",
  source = "dim_vehicle_make_view",
  keys = ["vehicle_make_id"],
  sequence_by = "vehicle_make_id",
  stored_as_scd_type = 1,
)

#Dim ride_status

@dp.view
def dim_ride_status_view():
  df = spark.readStream.table("silver_one_big_table")
  df = df.select("ride_status_id", "ride_status", "is_completed")
  df = df.dropDuplicates(subset = ["ride_status_id"])
  return df

#Create streaming table

dp.create_streaming_table("dim_ride_status")
dp.create_auto_cdc_flow(
  target = "dim_ride_status",
  source = "dim_ride_status_view",
  keys = ["ride_status_id"],
  sequence_by = "ride_status_id",
  stored_as_scd_type = 1,
)

#Fact table

@dp.view
def fact_rides_view():
  df = spark.readStream.table("silver_one_big_table")
  df = df.select("ride_id", "passenger_id", "driver_id", "vehicle_id", "pickup_location_id", "dropoff_location_id", "payment_method_id", "ride_status_id", "vehicle_type_id", "vehicle_make_id", "pickup_city_id", "dropoff_city_id", "cancellation_reason_id","distance_miles", "duration_minutes", "booking_timestamp", "pickup_timestamp", "dropoff_timestamp", "base_fare", "distance_fare", "time_fare", "surge_multiplier", "subtotal", "tip_amount", "total_fare", "rating")
  return df

#Create streaming table

dp.create_streaming_table("fact_rides")
dp.create_auto_cdc_flow(
  target = "fact_rides",
  source = "fact_rides_view",
  keys = ["ride_id", "passenger_id", "driver_id", "vehicle_id", "pickup_location_id", "dropoff_location_id", "payment_method_id", "ride_status_id", "vehicle_type_id", "vehicle_make_id", "pickup_city_id", "dropoff_city_id", "cancellation_reason_id"],
  sequence_by = "ride_id",
  stored_as_scd_type = 1,
)




  
                 






