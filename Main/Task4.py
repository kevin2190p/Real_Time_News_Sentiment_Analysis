from datetime import datetime
from StoreMongoDBData import StoreMongoDBData
from pyspark.sql import SparkSession
import time

# MongoDB configuration
MONGO_URI = "mongodb+srv://HongLik:SHL24112004shl@cluster0.dqhni.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
DATABASE_NAME = "sentiment_analysis_database"
COLLECTION_NAME = "sentiment_dataa"
PARQUET_FILE_PATH = 'Enriched_With_Date1.parquet'

# MongoDB Spark Connector Package
MONGO_SPARK_CONNECTOR_PACKAGE = "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0"

def main():
    """Main function to process Parquet data and load into MongoDB."""
    print("\n" + "="*80)
    print("🚀 Starting PySpark Parquet to MongoDB Loader")
    print("="*80)
    
    start_time = time.time()
    spark = None
    
    try:
        # Initialize Spark session
        print("\n📡 Initializing Spark session...")
        spark = SparkSession.builder \
            .appName("ParquetToMongoLoaderAndQuery") \
            .config("spark.jars.packages", MONGO_SPARK_CONNECTOR_PACKAGE) \
            .config("spark.mongodb.input.uri", MONGO_URI) \
            .config("spark.mongodb.output.uri", MONGO_URI) \
            .config("spark.mongodb.output.database", DATABASE_NAME) \
            .config("spark.mongodb.output.collection", COLLECTION_NAME) \
            .master("local[*]") \
            .getOrCreate()


        # Set log level
        spark.sparkContext.setLogLevel("WARN")
        
        print(f"✓ Spark session created successfully (version: {spark.version})")
        
        # Initialize the StoreMongoData class
        sentiment_analysis = StoreMongoData(MONGO_URI, DATABASE_NAME, COLLECTION_NAME, spark)

        # Test MongoDB connection
        if not sentiment_analysis.ping():
            print("❌ Cannot proceed without MongoDB connection. Exiting.")
            return
            
        # Read the parquet file
        df = sentiment_analysis.read_parquet(PARQUET_FILE_PATH)
        if df is None:
            print("❌ Error reading Parquet file. Exiting.")
            return
        
        # Transform the data
        df_transformed = sentiment_analysis.preprocess_and_transform(df)
        if df_transformed is None:
            print("❌ Error transforming data. Exiting.")
            return
        
        # Store the data in MongoDB
        records_inserted = sentiment_analysis.store_to_mongodb(df_transformed)
        if records_inserted == 0:
            print("⚠️ No records were inserted into MongoDB.")
        
        # Display example records
        sentiment_analysis.display_example_record(df_transformed)
        
        # Show collection statistics
        sentiment_analysis.get_collection_stats()
        
        # Calculate total execution time
        total_time = time.time() - start_time
        
        print("\n" + "="*80)
        print(f"✅ Process completed successfully in {total_time:.2f} seconds")
        print(f"✓ {records_inserted:,} records processed and stored in MongoDB")
        print("="*80 + "\n")
        
    except Exception as e:
        print("\n" + "="*80)
        print(f"❌ ERROR: An unexpected error occurred: {str(e)}")
        print("="*80 + "\n")
        
    finally:
        if spark:
            print("📌 Stopping Spark session...")
            spark.stop()
            print("✓ Spark session stopped")

if __name__ == "__main__":
    main()