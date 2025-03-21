# src/common.py

import dlt
from pyspark.sql import DataFrame

# Define quality expectations for silver layer


SILVER_EXPECTATIONS = {
    "valid_timestamp": "timestamp IS NOT NULL",
    "valid_turbine_id": "turbine_id IS NOT NULL",
    "valid_power_output": "power_output >= 0",
    "valid_wind_speed": "wind_speed BETWEEN 0 AND 40",
    "valid_wind_direction": "wind_direction BETWEEN 0 AND 360"
}

# Map layer names to their expectations
LAYER_EXPECTATIONS = {
    "silver": SILVER_EXPECTATIONS
}

def get_expectations(layer: str) -> dict:
    """
    Get the expectations dictionary for a specific layer.
    
    Args:
        layer (str): Layer name ('bronze' or 'silver')
    
    Returns:
        dict: Dictionary of expectations for the specified layer
    """
    return LAYER_EXPECTATIONS.get(layer, {})
