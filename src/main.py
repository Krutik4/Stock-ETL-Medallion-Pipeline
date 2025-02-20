from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from typing import Dict, Tuple
from data_processor import DataProcessor
from dotenv import load_dotenv
import os
import yaml
import glob
import shutil


def create_spark_session(mysql_connector_path: str, mongodb_uri: str) -> SparkSession:
    """
    Create and return a SparkSession with necessary configurations.

    :param mysql_connector_path: Path to MySQL JDBC connector JAR
    :param mongodb_uri: URI for MongoDB connection
    :return: Configured SparkSession
    """
    return (
        SparkSession.builder.appName("StockDataWarehouse")
        .config("spark.jars", mysql_connector_path)
        .config(
            "spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1"
        )
        .config("spark.mongodb.input.uri", mongodb_uri)
        .getOrCreate()
    )


def main(config: Dict[str, str]) -> Tuple[DataFrame, SparkSession]:
    """
    Main function to process stock data for data warehouse

    :param config: Configuration dictionary containing database and path information
    :return: Tuple of the processed DataFrame and SparkSession
    """
    spark = create_spark_session(config["mysql_connector_path"], config["mongodb_uri"])
    data_processor = DataProcessor(spark)

    # 1. Load all data
    print("Loading data from sources...")
    company_info = data_processor.load_company_info(config)
    market_index = data_processor.load_market_index(config)
    transactions = data_processor.load_transaction_data(config)

    # 2. Aggregate hourly data
    print("Aggregating hourly data...")
    hourly_transactions = data_processor.aggregate_hourly_transactions(transactions)
    hourly_market_index = data_processor.aggregate_hourly_market_index(market_index)

    # 3. Join datasets
    print("Joining datasets...")
    result_df = data_processor.join_datasets(
        company_info, hourly_transactions, hourly_market_index
    )

    # 4. Replace null values
    result_df = result_df.na.fill({"volume": 0})

    # 5. Save results and create time-based aggregations
    print("Creating time-based aggregations and saving results...")

    output_path = config.get("output_path", "data/answers")
    os.makedirs(output_path, exist_ok=True)

    # Process and save aggregations
    hourly_df = data_processor.aggregate_hourly(result_df)
    print("\nPreview of hourly_stock_data:")
    print(hourly_df.show(5))
    data_processor.save_to_csv(hourly_df, output_path, "hourly_stock_data.csv")

    daily_df = data_processor.aggregate_daily(result_df)
    print("\nPreview of daily_stock_data:")
    print(daily_df.show(5))
    data_processor.save_to_csv(daily_df, output_path, "daily_stock_data.csv")

    monthly_df = data_processor.aggregate_monthly(result_df)
    print("\nPreview of monthly_stock_data:")
    print(monthly_df.show(5))
    data_processor.save_to_csv(monthly_df, output_path, "monthly_stock_data.csv")

    quarterly_df = data_processor.aggregate_quarterly(result_df)
    print("\nPreview of quarterly_stock_data:")
    print(quarterly_df.show(5))
    data_processor.save_to_csv(quarterly_df, output_path, "quarterly_stock_data.csv")

    print("Processing completed successfully.")

    return result_df, spark


if __name__ == "__main__":
    # Load configuration
    load_dotenv()

    # Load and process config
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)
        # Replace environment variables
        for key, value in config.items():
            if (
                isinstance(value, str)
                and value.startswith("${")
                and value.endswith("}")
            ):
                env_var = value[2:-1]
                env_value = os.getenv(env_var)
                if env_value is None:
                    print(f"Warning: Environment variable {env_var} not found")
                config[key] = env_value or value

    print(f"Output path: {config.get('output_path')}")

    processed_df, spark = main(config)
    spark.stop()
