"""
Core Analytics Module
Computes analytics using native PySpark DataFrame operations for true distributed processing.
"""

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql.window import Window
from typing import Dict, Optional
import sys
import os

# Handle imports for both direct execution and module import
try:
    from src.spark.spark_session import get_spark_session, load_config
except ImportError:
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
    from src.spark.spark_session import get_spark_session, load_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def compute_total_log_count(df: DataFrame) -> int:
    """Compute total number of logs"""
    # Count the total number of rows in the DataFrame
    total = df.count()
    # Log the total count for debugging purposes
    logger.info(f"Total log count: {total}")
    # Return the computed total count
    return total


def compute_error_count(df: DataFrame) -> int:
    """Compute total number of errors"""
    # Filter the DataFrame to include only rows where 'log_level' is 'ERROR' and count them
    error_count = df.filter(F.col("log_level") == "ERROR").count()
    # Log the total error count
    logger.info(f"Total error count: {error_count}")
    # Return the count of error logs
    return error_count


def compute_error_rate(df: DataFrame) -> float:
    """Compute error rate (errors / total logs)"""
    # Helper call: Get the total number of logs
    total = compute_total_log_count(df)
    # Check if total is zero to avoid division by zero error
    if total == 0:
        return 0.0
    
    # Helper call: Get the count of error logs
    errors = compute_error_count(df)
    # Calculate the ratio of errors to total logs
    rate = errors / total
    # Log the calculated error rate as both decimal and percentage
    logger.info(f"Error rate: {rate:.4f} ({rate*100:.2f}%)")
    # Return the error rate
    return rate


def errors_by_type(df: DataFrame) -> DataFrame:
    """Group errors by error type using native PySpark"""
    logger.info("Computing errors by type...")
    
    # Chain Spark transformations to compute aggregation
    result = (
        # Filter: select only rows where log_level is ERROR
        df.filter(F.col("log_level") == "ERROR")
        # Group By: group the data by the unique values in 'error_type' column
        .groupBy("error_type")
        # Aggregation: count the number of occurrences for each group and rename column to 'count'
        .agg(F.count("*").alias("count"))
        # Sort: order the results by count in descending order (highest first)
        .orderBy(F.col("count").desc())
    )
    
    return result


def errors_by_severity(df: DataFrame) -> DataFrame:
    """Group errors by severity level using native PySpark"""
    logger.info("Computing errors by severity...")
    
    # Chain Spark transformations
    result = (
        # Filter: select only ERROR logs
        df.filter(F.col("log_level") == "ERROR")
        # Group By: group first by 'severity' then by 'log_level'
        .groupBy("severity", "log_level")
        # Aggregation: count entries per severity group
        .agg(F.count("*").alias("count"))
        # Sort: order by severity in descending order
        .orderBy(F.col("severity").desc())
    )
    
    return result


def errors_by_time(df: DataFrame, time_unit: str = "hour") -> DataFrame:
    """Group errors by time (hour/day) using native PySpark"""
    logger.info(f"Computing errors by {time_unit}...")
    
    # Filter to get only error logs first
    errors_df = df.filter(F.col("log_level") == "ERROR")
    
    if time_unit == "hour":
        # Process for hourly grouping
        result = (
            errors_df
            # Create 'date' column by casting timestamp to date type
            .withColumn("date", F.to_date("timestamp"))
            # Create 'hour' column by extracting hour component from timestamp
            .withColumn("hour", F.hour("timestamp"))
            # Group by both date and hour
            .groupBy("date", "hour")
            # Count errors in each hour window
            .agg(F.count("*").alias("error_count"))
            # Sort chronologically
            .orderBy("date", "hour")
        )
    else:  # day
        # Process for daily grouping
        result = (
            errors_df
            # Create 'date' column
            .withColumn("date", F.to_date("timestamp"))
            # Group simply by date
            .groupBy("date")
            # Count errors per day
            .agg(F.count("*").alias("error_count"))
            # Sort chronologically
            .orderBy("date")
        )
    
    return result


def top_n_errors(df: DataFrame, n: int = 10) -> DataFrame:
    """Get top N most frequent errors using native PySpark"""
    logger.info(f"Computing top {n} errors...")
    
    # Chain Spark transformations
    result = (
        # Filter: keep only ERROR logs
        df.filter(F.col("log_level") == "ERROR")
        # Group By: group by both 'error_type' and the full 'message' text
        .groupBy("error_type", "message")
        # Aggregation: count occurrences
        .agg(F.count("*").alias("count"))
        # Sort: descending order of frequency
        .orderBy(F.col("count").desc())
        # Limit: take only the top N rows
        .limit(n)
    )
    
    return result


def error_trends_over_time(df: DataFrame, window_size: str = "1h") -> DataFrame:
    """Compute error trends over time using PySpark window functions"""
    logger.info(f"Computing error trends with {window_size} windows...")
    
    # Parse window size (e.g., "1h" -> 1 hour)
    # Replace shorthand units with SQL-compatible interval strings (e.g. '1h' -> '1 hour')
    interval = window_size.replace("h", " hour").replace("d", " day").replace("m", " minute")
    
    # Perform time-windowed aggregation
    result = (
        # Filter: keep only ERROR logs
        df.filter(F.col("log_level") == "ERROR")
        # Window Iteration: create tumbling windows on 'timestamp' column of size 'interval'
        .withColumn("time_window", F.window("timestamp", interval))
        # Group By: group by the window buckets
        .groupBy("time_window")
        # Aggregation: count errors in each window bucket
        .agg(F.count("*").alias("error_count"))
        # Extraction: select the start time of the window for easier plotting/sorting
        .withColumn("time_window", F.col("time_window.start"))  # Extract window start
        # Sort: order results chronologically
        .orderBy("time_window")
    )
    
    return result


def errors_per_ip(df: DataFrame) -> DataFrame:
    """Group errors by IP address using native PySpark"""
    logger.info("Computing errors per IP...")
    
    # Chain Spark transformations
    result = (
        # Multi-condition Filter: ERROR level AND IP is not null AND IP is not empty string
        df.filter(
            (F.col("log_level") == "ERROR") & 
            (F.col("ip_address").isNotNull()) & 
            (F.col("ip_address") != "")
        )
        # Group By: group by IP address
        .groupBy("ip_address")
        # Aggregation: count errors per IP
        .agg(F.count("*").alias("error_count"))
        # Sort: descending (most problematic IPs first)
        .orderBy(F.col("error_count").desc())
    )
    
    return result


def errors_per_service(df: DataFrame) -> DataFrame:
    """Group errors by service using native PySpark"""
    logger.info("Computing errors per service...")
    
    # Chain Spark transformations
    result = (
        # Multi-condition Filter: ERROR level AND Service Name is valid
        df.filter(
            (F.col("log_level") == "ERROR") & 
            (F.col("service_name").isNotNull()) & 
            (F.col("service_name") != "")
        )
        # Group By: group by service name
        .groupBy("service_name")
        # Aggregation: count errors per service
        .agg(F.count("*").alias("error_count"))
        # Sort: descending order
        .orderBy(F.col("error_count").desc())
    )
    
    return result


def generate_summary_statistics(df: DataFrame) -> Dict:
    """Generate comprehensive summary statistics using native PySpark"""
    logger.info("Generating summary statistics...")
    
    # Helper calls: Get basic metrics using predefined functions
    total_logs = compute_total_log_count(df)
    total_errors = compute_error_count(df)
    error_rate = compute_error_rate(df)
    
    # Log level distribution: Group by log level, count, and bring result to driver (collect)
    log_level_df = df.groupBy("log_level").count().collect()
    # Convert list of Rows to dictionary
    log_level_counts = {row["log_level"]: row["count"] for row in log_level_df}
    
    # Unique counts: Use aggregation with countDistinct for conditional columns
    unique_counts = df.agg(
        # Count distinct IPs, ignoring empty strings
        F.countDistinct(F.when(F.col("ip_address") != "", F.col("ip_address"))).alias("unique_ips"),
        # Count distinct Services, ignoring empty strings
        F.countDistinct(F.when(F.col("service_name") != "", F.col("service_name"))).alias("unique_services"),
        # Count distinct Error Types, but only where log_level is ERROR
        F.countDistinct(
            F.when(F.col("log_level") == "ERROR", F.col("error_type"))
        ).alias("unique_error_types")
    ).collect()[0] # Collect result to driver and take first row
    
    # Compile dictionary of all metrics
    summary = {
        "total_logs": total_logs,
        "total_errors": total_errors,
        "error_rate": error_rate,
        "log_level_counts": log_level_counts,
        "unique_ips": unique_counts["unique_ips"] or 0,
        "unique_services": unique_counts["unique_services"] or 0,
        "unique_error_types": unique_counts["unique_error_types"] or 0
    }
    
    logger.info(f"Summary statistics generated: {summary}")
    return summary


def run_all_analytics(df: DataFrame, config: Optional[Dict] = None) -> Dict[str, DataFrame]:
    """Run all analytics and return results as DataFrames"""
    if config is None:
        config = load_config()
    
    top_n = config.get('analytics', {}).get('top_n_errors', 10)
    
    logger.info("Running all analytics...")
    
    # All functions now use native PySpark operations
    results = {
        "errors_by_type": errors_by_type(df),
        "errors_by_severity": errors_by_severity(df),
        "errors_by_hour": errors_by_time(df, "hour"),
        "errors_by_day": errors_by_time(df, "day"),
        "top_n_errors": top_n_errors(df, top_n),
        "error_trends": error_trends_over_time(df, "1h"),
        "errors_per_ip": errors_per_ip(df),
        "errors_per_service": errors_per_service(df)
    }
    
    # Cache results for potential reuse
    for name, result_df in results.items():
        result_df.cache()
        logger.info(f"Cached {name} results")
    
    logger.info("All analytics completed")
    return results


if __name__ == "__main__":
    from ingest_logs import ingest_logs
    from parse_logs import parse_logs
    
    spark = get_spark_session()
    df_raw = ingest_logs()
    df_parsed = parse_logs(df_raw)
    
    # Run analytics
    analytics_results = run_all_analytics(df_parsed)
    
    # Display results
    for name, result_df in analytics_results.items():
        print(f"\n{name}:")
        result_df.show(20, truncate=False)
