# Databricks notebook source

# COMMAND ----------
# Import required libraries
import dlt
import sys
# Adjust the path to point to project's root directory, where 'src' resides.
sys.path.append("/Workspace/Users/mujahidr@hotmail.co.uk/WindTurbineProject/src")

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from tasks.common import get_expectations
from tasks.gold import summarize_turbine_performance

# COMMAND ----------
turbine_schema = StructType([
    StructField("timestamp", StringType(), True), 
    StructField("turbine_id", StringType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("wind_direction", DoubleType(), True),
    StructField("power_output", DoubleType(), True)
])

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bronze Layer: Raw Data Ingestion

# COMMAND ----------
@dlt.table(
    name="bronze_turbine_data",
    comment="Raw turbine data ingested from source table with no transformations",
    table_properties={
        "quality": "bronze",
        "pipelines.reset.allowed": "true"
    }
)
def bronze_turbine_data():
    """
    Ingest raw turbine data from CSV files into bronze layer.
    No transformations - just raw data with metadata.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .schema(turbine_schema)  # Use explicitly defined schema
        .option("header", "true")
        .load("/Volumes/ci/turbines/raw/")
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", expr("_metadata.file_name"))
    )


# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver Layer: Cleaned Data

# COMMAND ----------
@dlt.table(
    name="silver_turbine_data",
    comment="Cleaned and validated turbine data"
)
@dlt.expect_all_or_drop(get_expectations("silver"))
def silver_turbine_data():
    """
    Clean and validate turbine data from bronze layer.
    Applies quality rules to ensure data meets business requirements.
    """
    return dlt.read("bronze_turbine_data")

# COMMAND ----------
@dlt.view(
    name="failed_records",
    comment="Records that failed validation rules"
)
def failed_records():
    """
    Create a view of records that failed silver layer validation.
    Useful for monitoring data quality issues.
    """
    return (
        dlt.read("bronze_turbine_data")
        .join(
            dlt.read("silver_turbine_data"),
            ["timestamp", "turbine_id"],
            "leftanti"
        )
    )


# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold Layer: Analytics

# COMMAND ----------
@dlt.table(
    name="gold_turbine_daily_stats",
    comment="Daily summary statistics and anomaly detection for each turbine"
)
def gold_turbine_daily_stats():
    """
    Generate daily performance metrics and anomaly detection for each turbine.
    """
    # Read the silver layer data (assumed to be already defined and populated)
    silver_df = dlt.read("silver_turbine_data")
    
    # Apply the transformation function.
    return summarize_turbine_performance(silver_df)



