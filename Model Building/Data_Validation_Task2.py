# Core Spark Imports
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, sum, length, count, regexp_extract, udf, lit, split, array_contains,
    size, to_date, coalesce, format_string, lpad, explode, isnull
)
from pyspark.sql.types import (
    IntegerType, StringType, ArrayType, StructType, StructField, FloatType,
    DateType, TimestampType
)

# ML and NLP Imports
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer
from pyspark.ml.clustering import LDA
import spacy
from pyspark.sql import functions as F

# Standard Python Libraries
import re
from urllib.parse import urlparse
from datetime import datetime
from collections import OrderedDict
import os


# ==================================================
# Class Definition: DataValidator
# ==================================================
class DataValidator_Task2:
    """
    Validates the input news articles DataFrame based on predefined rules.
    """
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.df = None
        self.validation_summary = {}
        print("DataValidator initialized.")

    def load_data(self, path: str) -> 'DataValidator':
        """Loads data from a Parquet file."""
        print(f"--- [Validator] Loading data from: {path} ---")
        try:
            self.df = self.spark.read.parquet(path)
            print(f"Successfully loaded data. Row count: {self.df.count()}")
            self.df.printSchema()
        except Exception as e:
            print(f"ERROR: Failed to load Parquet file from {path}: {e}")
            self.df = None # Ensure df is None if loading fails
            raise # Re-raise the exception to halt execution if loading fails
        return self

    def run_all_validations(self,
                            required_columns=["url", "content", "title"],
                            potential_date_columns=["publish_date", "date", "published_at", "timestamp", "creation_date"],
                            url_column="url",
                            content_column="content",
                            min_content_length=20) -> 'DataValidator':
        """Runs all defined validation checks."""
        if self.df is None:
            print("ERROR: DataFrame not loaded. Cannot run validations.")
            return self

        print("\n--- [Validator] Running Data Validation Checks ---")
        self._check_required_columns(required_columns)
        self._check_nulls_or_empty(required_columns + potential_date_columns)
        self._validate_url_format(url_column)
        self._check_content_length(content_column, min_content_length)
        self._check_duplicate_urls(url_column)
        print("--- [Validator] Validation checks completed ---")
        self._print_summary()
        return self

    def _check_required_columns(self, required_columns: list):
        """Checks if essential columns exist in the DataFrame."""
        print("Checking for required columns...")
        missing_cols = [c for c in required_columns if c not in self.df.columns]
        if missing_cols:
            self.validation_summary['missing_required_columns'] = missing_cols
            print(f"WARNING: Missing required columns: {missing_cols}")
        else:
            self.validation_summary['missing_required_columns'] = []
            print("All required columns are present.")

    def _check_nulls_or_empty(self, columns_to_check: list):
        """Checks for null or empty string values in specified columns."""
        print("Checking for null or empty values...")
        null_counts = {}
        existing_cols = [c for c in columns_to_check if c in self.df.columns]
        if existing_cols:
            results = self.df.select([
                sum(when(isnull(c) | (col(c) == ""), 1).otherwise(0)).alias(f"null_empty_{c}")
                for c in existing_cols
            ]).first().asDict()
            null_counts = {col_alias.replace("null_empty_", ""): count for col_alias, count in results.items()}
            self.validation_summary['null_empty_counts'] = null_counts
            print("Null/Empty counts:", null_counts)
        else:
            print("No columns found to check for nulls/emptiness.")
            self.validation_summary['null_empty_counts'] = {}

    def _validate_url_format(self, url_column: str):
        """Validates the URL format (basic http/https check)."""
        print(f"Validating URL format in column '{url_column}'...")
        if url_column in self.df.columns:
            url_pattern = r"^https?://.+"
            invalid_count = self.df.filter(~col(url_column).rlike(url_pattern)).count()
            self.validation_summary['invalid_url_format_count'] = invalid_count
            print(f"Rows with potentially invalid URL format: {invalid_count}")
        else:
            print(f"Skipping URL format validation: Column '{url_column}' not found.")
            self.validation_summary['invalid_url_format_count'] = 'N/A'

    def _check_content_length(self, content_column: str, min_length: int):
        """Checks if the content length meets a minimum requirement."""
        print(f"Checking content length in column '{content_column}' (min: {min_length})...")
        if content_column in self.df.columns:
            short_content_count = self.df.filter(
                col(content_column).isNotNull() & (length(col(content_column)) < min_length)
            ).count()
            self.validation_summary['short_content_count'] = short_content_count
            print(f"Rows with content shorter than {min_length} characters: {short_content_count}")
        else:
            print(f"Skipping content length check: Column '{content_column}' not found.")
            self.validation_summary['short_content_count'] = 'N/A'

    def _check_duplicate_urls(self, url_column: str):
        """Checks for duplicate values in the URL column."""
        print(f"Checking for duplicate URLs in column '{url_column}'...")
        if url_column in self.df.columns:
            total_rows = self.df.count()
            distinct_non_null_urls = self.df.filter(col(url_column).isNotNull()).select(url_column).distinct().count()
            non_null_rows = self.df.filter(col(url_column).isNotNull()).count()
            duplicate_count = non_null_rows - distinct_non_null_urls
            self.validation_summary['duplicate_url_count'] = duplicate_count
            self.validation_summary['distinct_url_count'] = distinct_non_null_urls
            print(f"Total non-null rows with URL: {non_null_rows}")
            print(f"Distinct non-null URLs: {distinct_non_null_urls}")
            print(f"Duplicate non-null URLs found: {duplicate_count}")
        else:
            print(f"Skipping duplicate URL check: Column '{url_column}' not found.")
            self.validation_summary['duplicate_url_count'] = 'N/A'
            self.validation_summary['distinct_url_count'] = 'N/A'

    def _print_summary(self):
        """Prints the collected validation summary."""
        print("\n--- Validation Summary ---")
        for key, value in self.validation_summary.items():
            print(f"{key}: {value}")
        print("--------------------------")

    def get_summary(self) -> dict:
        """Returns the validation summary dictionary."""
        return self.validation_summary

    def get_dataframe(self) -> DataFrame:
        """Returns the loaded DataFrame."""
        return self.df

    def get_clean_dataframe(self, drop_duplicates_url=True, filter_short_content=True, min_length=20) -> DataFrame:
        """Returns a DataFrame filtered based on validation checks."""
        if self.df is None:
            print("ERROR: Cannot get clean DataFrame, data not loaded.")
            return None

        print("\n--- [Validator] Applying basic cleaning ---")
        clean_df = self.df

        # Filter null/invalid URLs
        if "url" in clean_df.columns:
             url_pattern = r"^https?://.+"
             clean_df = clean_df.filter(col("url").isNotNull() & col("url").rlike(url_pattern))
             print(f"Rows after filtering null/invalid URLs: {clean_df.count()}")
             if drop_duplicates_url:
                 clean_df = clean_df.dropDuplicates(["url"])
                 print(f"Rows after dropping duplicate URLs: {clean_df.count()}")

        # Filter short content
        if filter_short_content and "content" in clean_df.columns:
             clean_df = clean_df.filter(col("content").isNotNull() & (length(col("content")) >= min_length))
             print(f"Rows after filtering short content (<{min_length}): {clean_df.count()}")

        print("--- [Validator] Cleaning finished ---")
        return clean_df
