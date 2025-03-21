from pyspark.sql import DataFrame
from pyspark.sql.functions import to_date, col, avg, stddev_pop, min, max

def summarize_turbine_performance(df: DataFrame) -> DataFrame:
    """
    Computes daily performance metrics for each turbine, including:
      - The minimum, maximum, and average power output.
      - A flag indicating whether an anomaly exists (i.e., if the maximum exceeds the average by more than 2 population standard deviations, 
        or the minimum falls below the average by more than 2 population standard deviations).
      
    Parameters:
      df (DataFrame): Source DataFrame with columns 'timestamp', 'turbine_id', and 'power_output'.
      
    Returns:
      DataFrame: A DataFrame with aggregated daily statistics and an anomaly indicator.
    """
    # Convert timestamp to date and group by date and turbine identifier.
    daily_stats = (
        df.withColumn("date", to_date(col("timestamp")))
          .groupBy("date", "turbine_id")
          .agg(
              min("power_output").alias("min_output"),
              max("power_output").alias("max_output"),
              avg("power_output").alias("avg_output"),
              stddev_pop("power_output").alias("stddev_output")
          )
    )
    
    # Flag an anomaly if max_output is more than 2 stddev above average or min_output is more than 2 stddev below average.
    result = daily_stats.withColumn(
        "contains_anomaly",
        (col("max_output") > col("avg_output") + 2 * col("stddev_output")) |
        (col("min_output") < col("avg_output") - 2 * col("stddev_output"))
    ).drop("stddev_output")
    
    return result
