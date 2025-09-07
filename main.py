from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, avg, count, desc, month, year, dayofmonth, row_number, rank, dense_rank, when, date_sub, max as spark_max, lit, to_timestamp
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType
import zipfile
import os
from pathlib import Path

def load_data_from_zip(spark):
    """Load CSV files from ZIP archives using only PySpark"""
    data_dir = Path("data")
    zip_files = list(data_dir.glob("*.zip"))
    dataframes = {}
    temp_files = []
    
    print(f"ZIP files found: {[f.name for f in zip_files]}")
    
    try:
        for zip_file in zip_files:
            print(f"\nProcessing {zip_file.name}...")
            
            with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                file_list = zip_ref.namelist()
                csv_files = [f for f in file_list if f.endswith('.csv') and not f.startswith('__MACOSX')]
                
                print(f"Valid CSV files in {zip_file.name}: {csv_files}")
                
                for csv_file in csv_files:
                    try:
                        print(f"Extracting and loading {csv_file}...")
                        
                        temp_path = data_dir / f"temp_{Path(csv_file).name}"
                        with zip_ref.open(csv_file) as source, open(temp_path, 'wb') as target:
                            target.write(source.read())
                        
                        temp_files.append(temp_path)
                        
                        # Charger avec PySpark et forcer la lecture avec cache()
                        spark_df = spark.read.csv(str(temp_path), header=True, inferSchema=True)
                        spark_df = spark_df.cache()  # Force le chargement en mémoire
                        
                        # Forcer l'exécution pour que les données soient réellement chargées
                        row_count = spark_df.count()
                        col_count = len(spark_df.columns)
                        
                        dataframes[Path(csv_file).name] = spark_df
                        
                        print(f"SUCCESS: {csv_file} loaded - {row_count} rows, {col_count} columns")
                        
                    except Exception as e:
                        print(f"ERROR loading {csv_file}: {e}")
        
        return dataframes
            
    finally:
        # Maintenant on peut supprimer les fichiers temporaires car les données sont en cache
        for temp_file in temp_files:
            try:
                if temp_file.exists():
                    os.remove(temp_file)
                    print(f"Temp file removed: {temp_file.name}")
            except Exception as e:
                print(f"Error removing {temp_file.name}: {e}")

def harmonize_schema(df, filename):
    """Harmonize different schemas from different years into a common format"""
    if "Divvy_Trips_2019_Q4.csv" in filename:
        # 2019 Q4 format: trip_id, start_time, end_time, bikeid, tripduration, from_station_id, from_station_name, to_station_id, to_station_name, usertype, gender, birthyear
        return df.select(
            col("trip_id").alias("ride_id"),
            lit("classic_bike").alias("rideable_type"),
            col("start_time").alias("started_at"),
            col("end_time").alias("ended_at"),
            col("from_station_name").alias("start_station_name"),
            col("from_station_id").alias("start_station_id"),
            col("to_station_name").alias("end_station_name"),
            col("to_station_id").alias("end_station_id"),
            lit(None).cast("double").alias("start_lat"),
            lit(None).cast("double").alias("start_lng"),
            lit(None).cast("double").alias("end_lat"),
            lit(None).cast("double").alias("end_lng"),
            col("usertype").alias("member_casual"),
            col("gender"),
            col("birthyear"),
            col("tripduration").cast("double").alias("tripduration")
        )
    elif "Divvy_Trips_2020_Q1.csv" in filename:
        # 2020 Q1 format: ride_id, rideable_type, started_at, ended_at, start_station_name, start_station_id, end_station_name, end_station_id, start_lat, start_lng, end_lat, end_lng, member_casual
        # Calculate trip duration in seconds from timestamps
        df_with_duration = df.withColumn("started_at_ts", to_timestamp(col("started_at"), "yyyy-MM-dd HH:mm:ss")) \
                             .withColumn("ended_at_ts", to_timestamp(col("ended_at"), "yyyy-MM-dd HH:mm:ss")) \
                             .withColumn("tripduration", 
                                       (col("ended_at_ts").cast("long") - col("started_at_ts").cast("long")).cast("double"))
        
        return df_with_duration.select(
            col("ride_id"),
            col("rideable_type"),
            col("started_at"),
            col("ended_at"),
            col("start_station_name"),
            col("start_station_id"),
            col("end_station_name"),
            col("end_station_id"),
            col("start_lat"),
            col("start_lng"),
            col("end_lat"),
            col("end_lng"),
            col("member_casual"),
            lit(None).cast("string").alias("gender"),
            lit(None).cast("integer").alias("birthyear"),
            col("tripduration")
        )
    else:
        return df

def prepare_data(df):
    """Prepare data by converting date columns and calculating age"""
    return df.withColumn("start_date", to_date(col("started_at"), "yyyy-MM-dd HH:mm:ss")) \
             .withColumn("end_date", to_date(col("ended_at"), "yyyy-MM-dd HH:mm:ss")) \
             .withColumn("month", month("start_date")) \
             .withColumn("year", year("start_date")) \
             .withColumn("day", dayofmonth("start_date")) \
             .withColumn("age", when(col("birthyear").isNotNull(), 2019 - col("birthyear")))

def average_trip_duration_per_day(df):
    """Calculate average trip duration per day"""
    print("Calculating average trip duration per day...")
    return df.groupBy("start_date") \
             .agg(avg("tripduration").alias("avg_trip_duration")) \
             .orderBy("start_date")

def trips_per_day(df):
    """Count trips per day"""
    print("Counting trips per day...")
    return df.groupBy("start_date") \
             .agg(count("*").alias("trip_count")) \
             .orderBy("start_date")

def most_popular_starting_station_per_month(df):
    """Find most popular starting station per month"""
    print("Finding most popular starting station per month...")
    window_spec = Window.partitionBy("year", "month").orderBy(desc("station_count"))
    
    return df.filter(col("start_station_id").isNotNull()) \
             .groupBy("year", "month", "start_station_id", "start_station_name") \
             .agg(count("*").alias("station_count")) \
             .withColumn("rank", row_number().over(window_spec)) \
             .filter(col("rank") == 1) \
             .select("year", "month", "start_station_id", "start_station_name", "station_count") \
             .orderBy("year", "month")

def top_3_stations_per_day_last_two_weeks(df):
    """Find top 3 trip stations each day for the last two weeks"""
    print("Finding top 3 stations per day for last two weeks...")
    
    # Get the last date in the dataset
    max_date = df.select(spark_max("start_date")).collect()[0][0]
    
    # Calculate 14 days before the last date
    two_weeks_ago = date_sub(max_date, 14)
    
    # Filter for last two weeks
    recent_df = df.filter(col("start_date") >= two_weeks_ago) \
                  .filter(col("start_station_id").isNotNull())
    
    window_spec = Window.partitionBy("start_date").orderBy(desc("station_count"))
    
    return recent_df.groupBy("start_date", "start_station_id", "start_station_name") \
                    .agg(count("*").alias("station_count")) \
                    .withColumn("rank", row_number().over(window_spec)) \
                    .filter(col("rank") <= 3) \
                    .select("start_date", "start_station_id", "start_station_name", "station_count", "rank") \
                    .orderBy("start_date", "rank")

def gender_trip_duration_analysis(df):
    """Compare average trip duration between males and females"""
    print("Analyzing trip duration by gender...")
    return df.filter(col("gender").isin(["Male", "Female"])) \
             .groupBy("gender") \
             .agg(avg("tripduration").alias("avg_trip_duration"),
                  count("*").alias("trip_count")) \
             .orderBy(desc("avg_trip_duration"))

def top_ages_longest_shortest_trips(df):
    """Find top 10 ages for longest and shortest trips"""
    print("Analyzing top 10 ages for longest and shortest trips...")
    
    # Filter out invalid ages (reasonable range)
    valid_age_df = df.filter((col("age") >= 16) & (col("age") <= 100))
    
    age_stats = valid_age_df.groupBy("age") \
                           .agg(avg("tripduration").alias("avg_trip_duration"),
                                count("*").alias("trip_count")) \
                           .filter(col("trip_count") >= 10)  # Only include ages with at least 10 trips
    
    # Top 10 longest
    longest = age_stats.orderBy(desc("avg_trip_duration")).limit(10) \
                      .withColumn("category", lit("longest"))
    
    # Top 10 shortest
    shortest = age_stats.orderBy("avg_trip_duration").limit(10) \
                       .withColumn("category", lit("shortest"))
    
    return longest.union(shortest)

def save_report(df, filename):
    """Save DataFrame as CSV report"""
    reports_dir = Path("reports")
    reports_dir.mkdir(exist_ok=True)
    
    output_path = reports_dir / filename
    print(f"Saving report to {output_path}")
    
    # Use coalesce(1) to create a single file
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(output_path))
    print(f"Report saved successfully!")

def run_all_analyses(dataframes):
    """Run all required analyses on the datasets"""
    print("\n" + "="*80)
    print("STARTING DATA ANALYSIS")
    print("="*80)
    
    # Harmonize and combine all dataframes
    combined_df = None
    for filename, df in dataframes.items():
        print(f"Harmonizing schema for {filename}...")
        harmonized_df = harmonize_schema(df, filename)
        
        if combined_df is None:
            combined_df = harmonized_df
        else:
            combined_df = combined_df.union(harmonized_df)
    
    if combined_df is None:
        print("No data to analyze!")
        return
    
    print(f"Combined dataset has {combined_df.count()} total rows")
    
    # Prepare data
    prepared_df = prepare_data(combined_df)
    prepared_df.cache()  # Cache for better performance
    
    try:
        # 1. Average trip duration per day
        avg_duration = average_trip_duration_per_day(prepared_df)
        save_report(avg_duration, "average_trip_duration_per_day.csv")
        print("Sample results:")
        avg_duration.show(10)
        
        # 2. Trips per day
        daily_trips = trips_per_day(prepared_df)
        save_report(daily_trips, "trips_per_day.csv")
        print("Sample results:")
        daily_trips.show(10)
        
        # 3. Most popular starting station per month
        popular_stations = most_popular_starting_station_per_month(prepared_df)
        save_report(popular_stations, "most_popular_starting_station_per_month.csv")
        print("Sample results:")
        popular_stations.show(10)
        
        # 4. Top 3 stations per day for last two weeks
        top_stations = top_3_stations_per_day_last_two_weeks(prepared_df)
        save_report(top_stations, "top_3_stations_last_two_weeks.csv")
        print("Sample results:")
        top_stations.show(15)
        
        # 5. Gender analysis
        gender_analysis = gender_trip_duration_analysis(prepared_df)
        save_report(gender_analysis, "gender_trip_duration_analysis.csv")
        print("Sample results:")
        gender_analysis.show()
        
        # 6. Age analysis
        age_analysis = top_ages_longest_shortest_trips(prepared_df)
        save_report(age_analysis, "top_ages_longest_shortest_trips.csv")
        print("Sample results:")
        age_analysis.show()
        
        print("\n" + "="*80)
        print("ALL ANALYSES COMPLETED SUCCESSFULLY!")
        print("Reports saved in the 'reports' folder")
        print("="*80)
        
    except Exception as e:
        print(f"Error during analysis: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("Starting Spark session...")
    
    # Configuration Spark plus légère pour Windows
    spark = SparkSession.builder \
        .appName("Exercise6") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()
    
    try:
        print("Loading data from ZIP files...")
        dataframes = load_data_from_zip(spark)
        
        if dataframes:
            print(f"\n{len(dataframes)} CSV file(s) loaded successfully!")
            
            for filename, df in dataframes.items():
                print(f"\n{'='*60}")
                print(f"ANALYSIS OF FILE: {filename}")
                print(f"{'='*60}")
                
                print(f"Data cached and ready for analysis")
                print(f"Number of columns: {len(df.columns)}")
                
                print(f"\nData schema:")
                df.printSchema()
                
                print(f"\nFirst 5 rows:")
                df.show(5, truncate=False)
            
            # Run all the required analyses
            run_all_analyses(dataframes)
            
        else:
            print("No data could be loaded.")
            
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        print("Stopping Spark session...")
        spark.stop()
        print("Done.")
