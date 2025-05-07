from pyspark.sql import SparkSession
from pymongo import MongoClient
from pyspark.sql.functions import col, when, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import json
import time


# Replace with your connection string and password
mongo_uri = "mongodb+srv://HongLik:SHL24112004shl@cluster0.dqhni.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

class StoreMongoDBData:
    def __init__(self, mongo_uri, parquet_file_path):
        """
        Initializes the StoreMongoDBData class.
        """
        self.parquet_file_path = parquet_file_path
        self.spark = self.create_spark_session()
        self.client = MongoClient(mongo_uri)
        self.db = self.client["sentiment_analysis_database"]
        self.collection = self.db["sentiment_dataa"]

    def create_spark_session(self):
        """
        Creates and configures the Spark session for MongoDB integration.
        """
        spark = SparkSession.builder \
            .appName("Real-Time Sentiment Analysis with MongoDB") \
            .config("spark.mongodb.input.uri", "mongodb+srv://...") \
            .config("spark.mongodb.output.uri", "mongodb+srv://...") \
            .config("spark.mongodb.output.database", "sentiment_analysis_database") \
            .config("spark.mongodb.output.collection", "sentiment_dataa") \
            .getOrCreate()
        return spark

    def read_parquet(self):
        """
        Reads the Parquet file into a Spark DataFrame.
        """
        df = self.spark.read.parquet(self.parquet_file_path)
        return df

    def add_sentiment_label(self, df):
        """
        Adds a sentiment label based on the sentiment score:
        - Positive (Sentiment_Result >= 1)
        - Neutral (Sentiment_Result == 0)
        - Negative (Sentiment_Result < 0)
        """
        df_transformed = df.withColumn("sentiment_label", 
                                       when(col("Sentiment_Result") >= 1, "positive")
                                       .when(col("Sentiment_Result") == 0, "neutral")
                                       .otherwise("negative"))
        return df_transformed

    def preprocess_and_transform(self, df):
        """
        Applies transformations like handling empty arrays and adding sentiment labels.
        """
        df_transformed = self.add_sentiment_label(df)
        return df_transformed

    def handle_empty_arrays(self, data):
        """
        Replaces empty arrays with None (null) for MongoDB compatibility.
        """
        for key, value in data.items():
            if isinstance(value, list) and len(value) == 0:
                data[key] = None
        return data

    def convert_to_dict(self, df):
        """
        Converts the Spark DataFrame to a list of dictionaries for MongoDB insertion.
        """
        records = df.select("publishedAt", "url", "cleaned_title", "source", "source_domain", 
                            "category", "word_count", "people_mentioned", "organizations_mentioned", 
                            "locations_mentioned", "sentence", "Sentiment_Result", "sentiment_label") \
                    .rdd.map(lambda row: row.asDict()).collect()
        
        # Preprocess the data (handle empty arrays)
        processed_records = [self.handle_empty_arrays(record) for record in records]
        
        return processed_records

    def store_to_mongodb(self, records):
        """
        Stores the processed records into MongoDB.
        """
        if records:
            self.collection.insert_many(records)
            print(f"{len(records)} records inserted into MongoDB.")

    def display_example_record(self, records):
        """
        Display a sample of the records to inspect the structure.
        """
        for record in records[:5]:  # print the first 5 records
            print("==========================================")
            for key, value in record.items():
                if isinstance(value, list):
                    print(f"{key}: {', '.join(map(str, value))}")
                else:
                    print(f"{key}: {value}")
            print("\n==========================================\n")


# Example usage
if __name__ == "__main__":
    mongo_uri = "mongodb+srv://HongLik:SHL24112004shl@cluster0.dqhni.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    parquet_file_path = 'Enriched_With_Date.parquet'  # Path to your parquet file
    
    # Step 1: Perform Sentiment Analysis and Store in MongoDB
    sentiment_analysis = StoreMongoDBData(mongo_uri, parquet_file_path)
    df = sentiment_analysis.read_parquet()
    df_transformed = sentiment_analysis.preprocess_and_transform(df)
    records = sentiment_analysis.convert_to_dict(df_transformed)
    sentiment_analysis.store_to_mongodb(records)
    sentiment_analysis.display_example_record(records)