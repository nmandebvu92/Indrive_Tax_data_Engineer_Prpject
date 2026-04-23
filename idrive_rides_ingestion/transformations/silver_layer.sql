CREATE OR REFRESH STREAMING TABLE silver_one_big_table
AS 

 
  SELECT
    
        ride_strm_table.ride_id, ride_strm_table.confirmation_number, ride_strm_table.passenger_id, ride_strm_table.driver_id, ride_strm_table.vehicle_id, ride_strm_table.pickup_location_id, ride_strm_table.dropoff_location_id, ride_strm_table.vehicle_type_id, ride_strm_table.vehicle_make_id, ride_strm_table.payment_method_id, ride_strm_table.ride_status_id, ride_strm_table.pickup_city_id, ride_strm_table.dropoff_city_id, ride_strm_table.cancellation_reason_id, ride_strm_table.passenger_name, ride_strm_table.passenger_email, ride_strm_table.passenger_phone, ride_strm_table.driver_name,ride_strm_table.driver_rating, ride_strm_table.driver_phone, ride_strm_table.driver_license, ride_strm_table.vehicle_model, ride_strm_table.vehicle_color, ride_strm_table.license_plate, ride_strm_table.pickup_address, ride_strm_table.pickup_latitude, ride_strm_table.pickup_longitude, ride_strm_table.dropoff_address, ride_strm_table.dropoff_latitude, ride_strm_table.dropoff_longitude, ride_strm_table.distance_miles, ride_strm_table.duration_minutes, ride_strm_table.booking_timestamp, ride_strm_table.pickup_timestamp, ride_strm_table.dropoff_timestamp, ride_strm_table.base_fare, ride_strm_table.distance_fare, ride_strm_table.time_fare, ride_strm_table.surge_multiplier, ride_strm_table.subtotal, ride_strm_table.tip_amount, ride_strm_table.total_fare, ride_strm_table.rating 
            
                , 
            
    
        map_cities.city as pickup_city, map_cities.state, map_cities.region 
            
                , 
            
    
        map_cancellation_reasons.cancellation_reason 
            
                , 
            
    
        map_payment_methods.payment_method, map_payment_methods.is_card, map_payment_methods.requires_auth 
            
                , 
            
    
        map_ride_statuses.ride_status, map_ride_statuses.is_completed 
            
                , 
            
    
        map_vehicle_makes.vehicle_make 
            
                , 
            
    
        map_vehicle_types.vehicle_type, map_vehicle_types.description, map_vehicle_types.base_rate, map_vehicle_types.per_mile, map_vehicle_types.per_minute 
            
    
From
    
        
           STREAM (indrive_catalog.indrive_schema.rides_strm_table) ride_strm_table
        
    
           left join indrive_catalog.indrive_schema.map_cities map_cities ON ride_strm_table.pickup_city_id = map_cities.city_id
        
    
        
           left join indrive_catalog.indrive_schema.map_cancellation_reasons map_cancellation_reasons ON ride_strm_table.cancellation_reason_id = map_cancellation_reasons.cancellation_reason_id
        
    
        
           left join indrive_catalog.indrive_schema.map_payment_methods map_payment_methods ON ride_strm_table.payment_method_id = map_payment_methods.payment_method_id
        
    
        
           left join indrive_catalog.indrive_schema.map_ride_statuses map_ride_statuses ON ride_strm_table.ride_status_id = map_ride_statuses.ride_status_id
        
    
        
           left join indrive_catalog.indrive_schema.map_vehicle_makes map_vehicle_makes ON ride_strm_table.vehicle_make_id = map_vehicle_makes.vehicle_make_id
        
    
        
           left join indrive_catalog.indrive_schema.map_vehicle_types map_vehicle_types ON ride_strm_table.vehicle_type_id = map_vehicle_types.vehicle_type_id
        
    

        
