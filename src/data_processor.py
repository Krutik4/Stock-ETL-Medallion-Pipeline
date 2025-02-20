from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    hour,
    sum,
    avg,
    to_timestamp,
    date_trunc,
    round as spark_round,
    explode,
    abs as spark_abs,
    quarter,
    year,
    concat,
    lit,
    count,
    month,
    date_format,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    ArrayType,
    IntegerType,
)
from typing import Dict
import glob
import shutil
import os


class DataProcessor:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def load_company_info(self, config: Dict[str, str]) -> DataFrame:
        """Load company information from MySQL"""
        return (
            self.spark.read.format("jdbc")
            .option("url", config["mysql_url"])
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .option("dbtable", "company_info")
            .option("user", config["mysql_user"])
            .option("password", config["mysql_password"])
            .load()
        )

    def load_transaction_data(self, config: Dict[str, str]) -> DataFrame:
        """Load transaction data from MongoDB"""
        df = (
            self.spark.read.format("mongo")
            .option("database", config["mongo_db"])
            .option("collection", config["mongo_collection"])
            .load()
        )

        # Transform the data
        return df.select(
            to_timestamp(col("timestamp")).alias("timestamp"),
            explode(col("transactions")).alias("transaction"),
        ).select(
            col("timestamp"),
            col("transaction.ticker"),
            col("transaction.shares").cast("integer").alias("shares"),
            col("transaction.price").cast("double").alias("price"),
        )

    def load_market_index(self, config: Dict[str, str]) -> DataFrame:
        """Load market index data from MySQL"""
        return (
            self.spark.read.format("jdbc")
            .option("url", config["mysql_url"])
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .option("dbtable", "market_index")
            .option("user", config["mysql_user"])
            .option("password", config["mysql_password"])
            .load()
        ).withColumn("timestamp", to_timestamp("timestamp"))

    def aggregate_hourly_transactions(self, transactions: DataFrame) -> DataFrame:
        """
        Aggregate transactions by hour, calculating volume and weighted average price
        Note: this is helper function, you do not have to use it
        """
        return transactions.groupBy(
            date_trunc("hour", "timestamp").alias("datetime"), "ticker"
        ).agg(
            sum("shares").alias("volume"),
            (sum(col("shares") * col("price")) / sum("shares")).alias(
                "avg_stock_price"
            ),
        )

    def aggregate_hourly_market_index(self, market_index: DataFrame) -> DataFrame:
        """Aggregate market index by hour"""
        return market_index.groupBy(
            date_trunc("hour", "timestamp").alias("datetime")
        ).agg(avg("index_value").alias("market_index"))

    def join_datasets(
        self,
        company_info: DataFrame,
        hourly_transactions: DataFrame,
        hourly_market_index: DataFrame,
    ) -> DataFrame:
        """Join all hourly datasets together"""
        return (
            hourly_transactions.join(company_info, "ticker")
            .join(hourly_market_index, "datetime")
            .select(
                col("datetime"),
                col("ticker"),
                col("company_name"),
                spark_round("avg_stock_price", 2).alias("avg_stock_price"),
                col("volume").cast("integer").alias("volume"),
                spark_round("market_index", 2).alias("market_index"),
            )
            .orderBy("datetime", "ticker")
        )

    def save_to_csv(self, df: DataFrame, output_path: str, filename: str) -> None:
        """
        Save DataFrame to a single CSV file.

        :param df: DataFrame to save
        :param output_path: Base directory path
        :param filename: Name of the CSV file
        """
        # Ensure output directory exists
        os.makedirs(output_path, exist_ok=True)

        # Create full path for the output file
        full_path = os.path.join(output_path, filename)
        print(f"Saving to: {full_path}")  # Debugging output

        # Create a temporary directory in the correct output path
        temp_dir = os.path.join(output_path, "_temp")
        print(f"Temporary directory: {temp_dir}")  # Debugging output

        # Save to temporary directory
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_dir)

        # Find the generated part file
        csv_file = glob.glob(f"{temp_dir}/part-*.csv")[0]

        # Move and rename it to the desired output path
        shutil.move(csv_file, full_path)

        # Clean up - remove the temporary directory
        shutil.rmtree(temp_dir)

    def process_data(self, config: Dict[str, str]) -> DataFrame:
        """
        @deprecated Use individual methods instead for better control flow
        """
        raise DeprecationWarning(
            "This method is deprecated. Please use individual methods for loading, "
            "aggregating, and joining data instead."
        )

    def aggregate_hourly(self, df: DataFrame) -> DataFrame:
        """Standardize the hourly data format"""
        return df.select(
            date_format(col("datetime"), "yyyy-MM-dd HH:mm:ss").alias("datetime"),
            col("ticker"),
            col("company_name"),
            col("avg_stock_price").alias("avg_price"),
            col("volume"),
            col("market_index"),
        ).orderBy("datetime", "ticker")

    def aggregate_daily(self, df: DataFrame) -> DataFrame:
        """Aggregate hourly data to daily summaries"""
        return (
            df.groupBy(
                date_trunc("day", "datetime").alias("tmp_date"),
                "ticker",
                "company_name",
            )
            .agg(
                avg("avg_stock_price").alias("avg_price"),
                sum("volume").alias("volume"),
                avg("market_index").alias("market_index"),
            )
            .select(
                date_format("tmp_date", "yyyy-MM-dd").alias("date"),
                col("ticker"),
                col("company_name"),
                spark_round("avg_price", 2).alias("avg_price"),
                col("volume").cast("integer").alias("volume"),
                spark_round("market_index", 2).alias("market_index"),
            )
            .orderBy("date", "ticker")
        )

    def aggregate_monthly(self, df: DataFrame) -> DataFrame:
        """Aggregate daily data to monthly summaries"""
        return (
            df.groupBy(
                date_trunc("month", "datetime").alias("tmp_date"),
                "ticker",
                "company_name",
            )
            .agg(
                avg("avg_stock_price").alias("avg_price"),
                sum("volume").alias("volume"),
                avg("market_index").alias("market_index"),
            )
            .select(
                date_format("tmp_date", "yyyy-MM").alias("month"),
                col("ticker"),
                col("company_name"),
                spark_round("avg_price", 2).alias("avg_price"),
                col("volume").cast("integer").alias("volume"),
                spark_round("market_index", 2).alias("market_index"),
            )
            .orderBy("month", "ticker")
        )

    def aggregate_quarterly(self, df: DataFrame) -> DataFrame:
        """Aggregate daily data to Q4 summary only"""
        return (
            df.filter(
                month(col("datetime")).isin(
                    10, 11, 12
                )  # Only October, November, December
            )
            .withColumn("quarter", concat(year("datetime"), lit(" Q4")))
            .groupBy(col("quarter"), "ticker", "company_name")
            .agg(
                avg("avg_stock_price").alias("avg_price"),
                sum("volume").alias("volume"),
                avg("market_index").alias("market_index"),
            )
            .select(
                col("quarter"),
                col("ticker"),
                col("company_name"),
                spark_round("avg_price", 2).alias("avg_price"),
                col("volume").cast("integer").alias("volume"),
                spark_round("market_index", 2).alias("market_index"),
            )
            .orderBy("quarter", "ticker")
        )
