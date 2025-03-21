import sys
import os
# Ensure src is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))
import pytest
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, DateType, BooleanType
from pyspark.sql import DataFrame
from chispa.dataframe_comparer import assert_df_equality
from src.tasks.gold import summarize_turbine_performance  

class TestSummarizeTurbinePerformance:

    @pytest.fixture(scope="class")
    def spark(self):
        return SparkSession.builder.master("local[*]").appName("test").getOrCreate()

    def test_no_anomalies(self, spark):
        source_data = [
            ("2025-03-10 12:00:00", 1, 10.0),
            ("2025-03-10 13:00:00", 1, 12.0),
            ("2025-03-10 14:00:00", 1, 11.0)
        ]
        source_schema = StructType([
            StructField("timestamp", StringType(), True),
            StructField("turbine_id", LongType(), True),
            StructField("power_output", DoubleType(), True)
        ])
        source_df = spark.createDataFrame(source_data, schema=source_schema)

        expected_data = [
            (date(2025, 3, 10), 1, 10.0, 12.0, 11.0, False)  # min, max, avg, no anomaly
        ]
        expected_schema = StructType([
            StructField("date", DateType(), True),
            StructField("turbine_id", LongType(), True),
            StructField("min_output", DoubleType(), True),
            StructField("max_output", DoubleType(), True),
            StructField("avg_output", DoubleType(), True),
            StructField("contains_anomaly", BooleanType(), True)
        ])
        expected_df = spark.createDataFrame(expected_data, schema=expected_schema)

        actual_df = summarize_turbine_performance(source_df)

        assert_df_equality(actual_df, expected_df, ignore_nullable=True)

    def test_with_anomalies(self, spark):
        source_data = [
            ("2025-03-10 12:00:00", 1, 10.0),   # Normal
            ("2025-03-10 13:00:00", 1, 12.0),   # Normal
            ("2025-03-10 14:00:00", 1, 11.0),   # Normal
            ("2025-03-10 15:00:00", 1, 11.5),   # Normal
            ("2025-03-10 16:00:00", 1, 10.5),   # Normal
            ("2025-03-10 17:00:00", 1, 1000.0)  # Large spike (anomaly)
        ]
        source_schema = StructType([
            StructField("timestamp", StringType(), True),
            StructField("turbine_id", LongType(), True),
            StructField("power_output", DoubleType(), True)
        ])
        source_df = spark.createDataFrame(source_data, schema=source_schema)

        expected_data = [
            (date(2025, 3, 10), 1, 10.0, 1000.0, 175.83333333333334, True)  # Anomaly detected
        ]
        expected_schema = StructType([
            StructField("date", DateType(), True),
            StructField("turbine_id", LongType(), True),
            StructField("min_output", DoubleType(), True),
            StructField("max_output", DoubleType(), True),
            StructField("avg_output", DoubleType(), True),
            StructField("contains_anomaly", BooleanType(), True)
        ])
        expected_df = spark.createDataFrame(expected_data, schema=expected_schema)

        actual_df = summarize_turbine_performance(source_df)

        assert_df_equality(actual_df, expected_df, ignore_nullable=True)
