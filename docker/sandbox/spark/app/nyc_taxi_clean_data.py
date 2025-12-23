from pyspark.sql import SparkSession, functions as f

def main():
    spark = (
        SparkSession.builder
        .appName("NYC_Taxi_Clean_Data")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )

    try:
        # Disable Hadoop permission checks to avoid chmod errors in containerized environment
        spark.sparkContext._jsc.hadoopConfiguration().setBoolean("fs.permissions.enabled", False)
        
        # Read the output from nyc_taxi_read_all_years.py
        print("Reading staging output...")
        df = spark.read.parquet("/tmp/Staging_output")
        original_count = df.count()
        print(f"Loaded {original_count} records from staging output")
        
        # Step 1: Rename columns and add trip_duration_min
        print("Step 1: Renaming columns and calculating trip duration...")
        df_col_renamed = (
            df
            .withColumnRenamed("VendorID", "Vendor_ID")
            .withColumnRenamed("RatecodeID", "Ratecode_ID")
            .withColumnRenamed("PULocationID", "Pickup_Location_ID")
            .withColumnRenamed("DOLocationID", "Dropoff_Location_ID")
            .withColumnRenamed("extra", "extra_charges")
            .withColumnRenamed("tpep_pickup_datetime", "Trip_Pickup_DateTime")
            .withColumnRenamed("tpep_dropoff_datetime", "Trip_Dropoff_DateTime")
            .withColumn(
                'trip_duration_min',
                f.round(
                    (f.unix_timestamp('Trip_Dropoff_DateTime') - f.unix_timestamp('Trip_Pickup_DateTime')) / 60,
                    2
                )
            )
        )
        
        # Step 2: Fill null values
        print("Step 2: Filling null values...")
        mean_tip = df_col_renamed.select(f.mean("tip_amount")).collect()[0][0]
        
        df_no_nulls = df_col_renamed.fillna({
            'Vendor_ID': 99,
            'Ratecode_ID': 99,
            'store_and_fwd_flag': 'Unknown',
            'fare_amount': 0.0,
            'extra_charges': 0.0,
            'mta_tax': 0.0,
            'tip_amount': mean_tip,
            'cbd_congestion_fee': 0.0
        })
        
        # Step 3: Filter bad data
        print("Step 3: Filtering bad data...")
        df_filtered = df_no_nulls.filter(
            (f.col("passenger_count").between(1, 6)) &
            (f.col("trip_distance").between(0.1, 200)) &
            (f.col("Pickup_Location_ID") != f.col("Dropoff_Location_ID")) &
            (f.col("trip_duration_min").between(0.01, 120.1)) &
            (f.col("cbd_congestion_fee") >= 0.0) &
            (f.col("Airport_fee") >= 0.0) &
            (f.col("total_amount") >= 0.0) &
            (f.col("tip_amount") >= 0.0) &
            (f.col("fare_amount") >= 0.0) &
            (f.col("extra_charges") >= 0.0) &
            (f.col("improvement_surcharge") >= 0.0) &
            (f.col("mta_tax") >= 0.0)
        )
        
        # Step 4: Add derived columns
        print("Step 4: Adding derived columns...")
        df_derived_col = (
            df_filtered
            .withColumn(
                "Vendor_Name",
                f.when(f.col("Vendor_ID") == 1, f.lit("Creative Mobile Technologies, LLC"))
                 .when(f.col("Vendor_ID") == 2, f.lit("Curb Mobility, LLC"))
                 .when(f.col("Vendor_ID") == 6, f.lit("Myle Technologies Inc"))
                 .when(f.col("Vendor_ID") == 7, f.lit("Helix"))
                 .otherwise(f.lit("Unknown"))
            )
            .withColumn(
                "Ratecode_Description",
                f.when(f.col("Ratecode_ID") == 1, f.lit("Standard rate"))
                 .when(f.col("Ratecode_ID") == 2, f.lit("JFK"))
                 .when(f.col("Ratecode_ID") == 3, f.lit("Newark"))
                 .when(f.col("Ratecode_ID") == 4, f.lit("Nassau or Westchester"))
                 .when(f.col("Ratecode_ID") == 5, f.lit("Negotiated fare"))
                 .when(f.col("Ratecode_ID") == 6, f.lit("Group ride"))
                 .otherwise(f.lit("Unknown"))
            )
            .withColumn(
                "Payment_Method",
                f.when(f.col("payment_type") == 0, f.lit("Flex Fare trip"))
                 .when(f.col("payment_type") == 1, f.lit("Credit Card"))
                 .when(f.col("payment_type") == 2, f.lit("Cash"))
                 .when(f.col("payment_type") == 3, f.lit("No Charge"))
                 .when(f.col("payment_type") == 4, f.lit("Dispute"))
                 .when(f.col("payment_type") == 5, f.lit("Unknown"))
                 .when(f.col("payment_type") == 6, f.lit("Voided trip"))
                 .otherwise(f.lit("Unknown"))
            )
            .withColumn(
                "Trip_Distance_Km",
                f.round(f.col("trip_distance") * 1.609, 2)
            )
            .withColumn(
                "Year",
                f.year(f.col('Trip_Pickup_DateTime'))
            )
            .withColumn(
                "Month",
                f.date_format(f.col("Trip_Pickup_DateTime"), "MMM")
            )
            .withColumnRenamed('trip_distance', 'trip_distance_miles')
        )
        
        # Step 5: Read lookup zones
        print("Step 5: Reading lookup zones...")
        lookup_zones_df = (
            spark.read
            .option("header", "true")
            .option("inferschema", "True")
            .csv("/opt/spark/resources/taxi_zone_lookup.csv")
        )
        
        # Step 6: Join with lookup zones
        print("Step 6: Joining with lookup zones...")
        pickup_zone = lookup_zones_df.alias('pickup')
        dropoff_zone = lookup_zones_df.alias('dropoff')
        
        df_joined = df_derived_col \
            .join(
                pickup_zone,
                df_derived_col['Pickup_Location_ID'] == pickup_zone['LocationID'],
                how='left'
            ) \
            .join(
                dropoff_zone,
                df_derived_col['Dropoff_Location_ID'] == dropoff_zone['LocationID'],
                how='left'
            ) \
            .select(
                df_derived_col['*'],  # all original columns
                f.col('pickup.Borough').alias('Pickup_Borough'),
                f.col('pickup.Zone').alias('Pickup_Zone'),
                f.col('pickup.service_zone').alias('Pickup_Service_Zone'),
                f.col('dropoff.Borough').alias('Dropoff_Borough'),
                f.col('dropoff.Zone').alias('Dropoff_Zone'),
                f.col('dropoff.service_zone').alias('Dropoff_Service_Zone')
            )
        
        # Step 7: Select final columns
        print("Step 7: Selecting final columns...")
        final_df = df_joined.select(
            'Vendor_ID',
            'Vendor_Name',
            'Trip_Pickup_DateTime',
            'Trip_Dropoff_DateTime',
            'passenger_count',
            'Pickup_Location_ID',
            'Pickup_Borough',
            'Pickup_Zone',
            'Pickup_Service_Zone',
            'Dropoff_Location_ID',
            'Dropoff_Borough',
            'Dropoff_Zone',
            'Dropoff_Service_Zone',
            'Trip_Distance_Km',
            'trip_distance_miles',
            'Ratecode_ID',
            'Ratecode_Description',
            'payment_type',
            'Payment_Method',
            'trip_duration_min',
            'fare_amount',
            'extra_charges',
            'mta_tax',
            'tip_amount',
            'tolls_amount',
            'improvement_surcharge',
            'total_amount',
            'congestion_surcharge',
            'Airport_fee',
            'cbd_congestion_fee',
            'Year',
            'Month'
        )
        
        # Step 8: Print statistics
        print("Step 8: Calculating statistics...")
        final_count = final_df.count()
        
        print(f"Number of records of original df: {original_count}")
        print(f"Number of records after Cleaning Data: {final_count}")
        print(f"Number of filtered records: {original_count - final_count}")
        print(f"Percentage of bad data from whole data: {(original_count - final_count) / original_count * 100:.2f}%")
        
        # Step 9: Save cleaned dataframe
        print("Step 9: Saving cleaned dataframe to /tmp/Cleaned_data...")
        final_df.coalesce(1).write.mode("overwrite").parquet("/tmp/Cleaned_data")
        
        print("SUCCESS: Data cleaning completed and saved to /tmp/Cleaned_data")
        

        spark.stop()

if __name__ == "__main__":
    main()


