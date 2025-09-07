import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import lit
from datetime import datetime
import tempfile
import shutil
from pathlib import Path

from main import (
    prepare_data, 
    average_trip_duration_per_day, 
    trips_per_day, 
    gender_trip_duration_analysis,
    most_popular_starting_station_per_month,
    top_ages_longest_shortest_trips
)

@pytest.fixture(scope="session")
def spark():
    """Create Spark session for testing"""
    spark = SparkSession.builder \
        .appName("Exercise6Tests") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture
def sample_data(spark):
    """Create sample test data"""
    schema = StructType([
        StructField("trip_id", IntegerType(), True),
        StructField("start_time", StringType(), True),
        StructField("end_time", StringType(), True),
        StructField("bikeid", IntegerType(), True),
        StructField("tripduration", DoubleType(), True),
        StructField("from_station_id", IntegerType(), True),
        StructField("from_station_name", StringType(), True),
        StructField("to_station_id", IntegerType(), True),
        StructField("to_station_name", StringType(), True),
        StructField("usertype", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("birthyear", IntegerType(), True),
    ])
    
    data = [
        (1, "2019-10-01 08:00:00", "2019-10-01 08:15:00", 100, 900.0, 10, "Station A", 20, "Station B", "Subscriber", "Male", 1990),
        (2, "2019-10-01 09:00:00", "2019-10-01 09:10:00", 101, 600.0, 11, "Station C", 21, "Station D", "Customer", "Female", 1985),
        (3, "2019-10-02 08:30:00", "2019-10-02 08:45:00", 102, 900.0, 10, "Station A", 22, "Station E", "Subscriber", "Male", 1995),
        (4, "2019-10-02 10:00:00", "2019-10-02 10:20:00", 103, 1200.0, 12, "Station F", 23, "Station G", "Customer", "Female", 1980),
        (5, "2019-11-01 07:00:00", "2019-11-01 07:05:00", 104, 300.0, 10, "Station A", 24, "Station H", "Subscriber", "Male", 1975),
    ]
    
    return spark.createDataFrame(data, schema)

class TestDataPreparation:
    def test_prepare_data_adds_date_columns(self, spark, sample_data):
        """Test that prepare_data adds necessary date and age columns"""
        result = prepare_data(sample_data)
        
        # Check that new columns are added
        expected_columns = ["start_date", "end_date", "month", "year", "day", "age"]
        for col_name in expected_columns:
            assert col_name in result.columns
        
        # Check that age is calculated correctly
        ages = result.select("age").collect()
        expected_ages = [29, 34, 24, 39, 44]  # 2019 - birth years
        actual_ages = [row.age for row in ages]
        assert actual_ages == expected_ages

class TestTripAnalyses:
    def test_average_trip_duration_per_day(self, spark, sample_data):
        """Test average trip duration calculation per day"""
        prepared_data = prepare_data(sample_data)
        result = average_trip_duration_per_day(prepared_data)
        
        # Should have 2 distinct days
        assert result.count() == 2
        
        # Check that we have the expected columns
        assert "start_date" in result.columns
        assert "avg_trip_duration" in result.columns

    def test_trips_per_day(self, spark, sample_data):
        """Test trip count per day"""
        prepared_data = prepare_data(sample_data)
        result = trips_per_day(prepared_data)
        
        # Should have 2 distinct days
        assert result.count() == 2
        
        # Check the structure
        assert "start_date" in result.columns
        assert "trip_count" in result.columns
        
        # Check actual counts
        results_list = result.collect()
        trip_counts = [row.trip_count for row in results_list]
        assert sorted(trip_counts) == [1, 2]  # 2 trips on Oct 1, 1 trip on Nov 1

    def test_gender_trip_duration_analysis(self, spark, sample_data):
        """Test gender-based trip duration analysis"""
        prepared_data = prepare_data(sample_data)
        result = gender_trip_duration_analysis(prepared_data)
        
        # Should have 2 genders
        assert result.count() == 2
        
        # Check columns
        assert "gender" in result.columns
        assert "avg_trip_duration" in result.columns
        assert "trip_count" in result.columns

class TestStationAnalyses:
    def test_most_popular_starting_station_per_month(self, spark, sample_data):
        """Test most popular starting station per month"""
        prepared_data = prepare_data(sample_data)
        result = most_popular_starting_station_per_month(prepared_data)
        
        # Should have 2 months (October and November)
        assert result.count() == 2
        
        # Check columns
        expected_cols = ["year", "month", "from_station_id", "from_station_name", "station_count"]
        for col_name in expected_cols:
            assert col_name in result.columns

class TestAgeAnalysis:
    def test_top_ages_longest_shortest_trips(self, spark, sample_data):
        """Test age analysis for longest and shortest trips"""
        prepared_data = prepare_data(sample_data)
        
        # Add more data to make the test meaningful
        additional_data = []
        for i in range(10):
            for age_offset in [0, 10, 20, 30]:
                birth_year = 1980 - age_offset
                duration = 500.0 + (i * 100) + (age_offset * 50)
                additional_data.append((
                    100 + i, f"2019-10-0{(i % 9) + 1} 08:00:00", f"2019-10-0{(i % 9) + 1} 08:15:00",
                    200 + i, duration, 10, "Station A", 20, "Station B", 
                    "Subscriber", "Male", birth_year
                ))
        
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
        schema = sample_data.schema
        additional_df = spark.createDataFrame(additional_data, schema)
        combined_data = sample_data.union(additional_df)
        
        prepared_data = prepare_data(combined_data)
        result = top_ages_longest_shortest_trips(prepared_data)
        
        # Should return results for both longest and shortest categories
        categories = [row.category for row in result.collect()]
        assert "longest" in categories
        assert "shortest" in categories
        
        # Check columns
        expected_cols = ["age", "avg_trip_duration", "trip_count", "category"]
        for col_name in expected_cols:
            assert col_name in result.columns

class TestIntegration:
    def test_end_to_end_workflow(self, spark, sample_data):
        """Test that the complete workflow runs without errors"""
        # Prepare data
        prepared_data = prepare_data(sample_data)
        
        # Run all analyses
        analyses = [
            average_trip_duration_per_day,
            trips_per_day,
            gender_trip_duration_analysis,
            most_popular_starting_station_per_month,
        ]
        
        for analysis_func in analyses:
            result = analysis_func(prepared_data)
            assert result is not None
            assert result.count() > 0

if __name__ == "__main__":
    pytest.main([__file__])
