#!/usr/bin/env python3
"""
Quick test script to verify our main functions work
"""
from main import *

if __name__ == "__main__":
    print("Testing our functions...")
    
    # Test if we can import and create a basic Spark session
    try:
        spark = SparkSession.builder \
            .appName("QuickTest") \
            .config("spark.driver.memory", "1g") \
            .config("spark.executor.memory", "1g") \
            .getOrCreate()
        
        print("✅ Spark session created successfully")
        
        # Test data loading
        try:
            dataframes = load_data_from_zip(spark)
            print(f"✅ Data loading successful: {len(dataframes)} files loaded")
            
            # Test schema harmonization
            for filename, df in dataframes.items():
                harmonized = harmonize_schema(df, filename)
                print(f"✅ Schema harmonization for {filename}: {len(harmonized.columns)} columns")
                
        except Exception as e:
            print(f"❌ Error in data processing: {e}")
        
        spark.stop()
        print("✅ Test completed")
        
    except Exception as e:
        print(f"❌ Error creating Spark session: {e}")
