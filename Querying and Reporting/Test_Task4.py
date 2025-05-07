from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, PyMongoError
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, lit, expr
import pprint
import time
from datetime import datetime

class StoreMongoData:
    """Class for loading and transforming Parquet data to MongoDB."""
    
    def __init__(self, mongo_uri, database_name, collection_name, spark):
        """
        Initialize the StoreMongoData class with connection parameters.
        
        Args:
            mongo_uri (str): MongoDB connection URI
            database_name (str): Target database name
            collection_name (str): Target collection name
            spark (SparkSession): Active Spark session
        """
        self.mongo_uri = mongo_uri
        self.client = MongoClient(mongo_uri)
        self.database = self.client[database_name]
        self.collection = self.database[collection_name]
        self.spark = spark
            
    def ping(self):
        """
        Test MongoDB connection and print database information.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            self.client.admin.command('ping')
            print("=" * 80)
            print("✅ MongoDB Connection Status:")
            print(f"• Connection: SUCCESSFUL")
            print(f"• Current Database: {self.database.name}")
            
            # Get collections
            collections = self.database.list_collection_names()
            print(f"• Collections: {', '.join(collections) if collections else 'None'}")
            print("=" * 80)
            return True
        except ConnectionFailure as e:
            print("=" * 80)
            print("❌ ERROR: Unable to connect to MongoDB")
            print(f"• Error details: {str(e)}")
            print("=" * 80)
            return False
            
    def read_parquet(self, parquet_file_path):
        """
        Read the Parquet file into a Spark DataFrame.
        
        Args:
            parquet_file_path (str): Path to the Parquet file
            
        Returns:
            DataFrame: Spark DataFrame containing the data
        """
        try:
            start_time = time.time()
            
            print(f"\n📂 Reading Parquet file: {parquet_file_path}")
            df = self.spark.read.parquet(parquet_file_path)
            
            # Get summary statistics
            row_count = df.count()
            column_count = len(df.columns)
            elapsed_time = time.time() - start_time
            
            # Print summary information
            print("\n" + "=" * 80)
            print("📊 PARQUET FILE LOAD SUMMARY")
            print("=" * 80)
            print(f"• File Path: {parquet_file_path}")
            print(f"• Rows Loaded: {row_count:,}")
            print(f"• Columns: {column_count}")
            print(f"• Load Time: {elapsed_time:.2f} seconds")
            print("=" * 80)
            
            # Print schema
            print("\n📋 DataFrame Schema:")
            df.printSchema()
            
            # Display sample data
            print("\n📝 Sample Data (First 10 rows):")
            df.show(10, truncate=False)
            
            return df
            
        except Exception as e:
            print("\n" + "=" * 80)
            print("❌ ERROR: Failed to read Parquet file")
            print(f"• Error details: {str(e)}")
            print("=" * 80)
            return None

    def preprocess_and_transform(self, df):
        """
        Add sentiment label based on sentiment score and perform necessary transformations.
        
        Args:
            df (DataFrame): Input Spark DataFrame
            
        Returns:
            DataFrame: Transformed DataFrame with added columns
        """
        if df is None:
            print("❌ ERROR: DataFrame is None. Cannot transform data.")
            return None
            
        try:
            start_time = time.time()
            print("\n🔄 Transforming data and adding sentiment labels...")
            
            # Add sentiment label column
            df_transformed = df.withColumn(
                "sentiment_label",
                when(col("Sentiment_Result") > 0, "positive")
                .when(col("Sentiment_Result") == 0, "neutral")
                .otherwise("negative")
            )
            
            # Add processing timestamp
            df_transformed = df_transformed.withColumn("processed_at", 
                                                       lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
            
            # Calculate sentiment distribution for reporting
            sentiment_counts = df_transformed.groupBy("sentiment_label").count()
            
            # Print sentiment distribution
            print("\n" + "=" * 80)
            print("📊 SENTIMENT DISTRIBUTION")
            print("=" * 80)
            sentiment_counts.show()
            
            # Get row count for percentage calculation
            total_rows = df_transformed.count()
            
            # Add percentage column using Spark
            sentiment_counts_with_pct = sentiment_counts.withColumn(
                "percentage", 
                expr("ROUND(count * 100 / " + str(total_rows) + ", 2)")
            )
            
            # Show with percentages
            print("\n📊 SENTIMENT DISTRIBUTION (WITH PERCENTAGES)")
            sentiment_counts_with_pct.show()
            
            elapsed_time = time.time() - start_time
            print(f"✅ Transformation completed in {elapsed_time:.2f} seconds")
            
            return df_transformed
            
        except Exception as e:
            print("\n" + "=" * 80)
            print("❌ ERROR: Failed to transform data")
            print(f"• Error details: {str(e)}")
            print("=" * 80)
            return None

    def store_to_mongodb(self, df_transformed):
        """
        Store the processed records into MongoDB.
        
        Args:
            df_transformed (DataFrame): Transformed DataFrame to store
            
        Returns:
            int: Number of records inserted
        """
        if df_transformed is None:
            print("❌ ERROR: No data to store in MongoDB.")
            return 0
            
        try:
            start_time = time.time()
            print("\n💾 Preparing to store data in MongoDB...")
            
            # Select relevant columns
            selected_df = df_transformed.select(
                "publishedAt", "url", "cleaned_title", "cleaned_description", "source",
                "source_domain", "category", "word_count", "people_mentioned",
                "organizations_mentioned", "locations_mentioned", "sentence",
                "Sentiment_Result", "sentiment_label", "processed_at"
            )
            
            # Convert to Python dictionaries using Spark
            records = selected_df.rdd.map(lambda row: row.asDict()).collect()
            
            # Add insertion timestamp to each record
            for record in records:
                record['inserted_at'] = datetime.now()
            
            # Use batching for efficient insertion
            batch_size = 250
            total_records = len(records)
            total_inserted = 0
            
            print(f"\n🔄 Inserting {total_records:,} records in batches of {batch_size}...")
            print("=" * 80)
            print(f"{'BATCH':<10} {'RECORDS':<10} {'PROGRESS':<20} {'TIME':<10}")
            print("=" * 80)
            
            # Process in batches
            batch_num = 1
            for i in range(0, total_records, batch_size):
                batch_start_time = time.time()
                batch = records[i:i + batch_size]
                
                # Insert the batch
                self.collection.insert_many(batch)
                
                # Update counters
                total_inserted += len(batch)
                progress_pct = (total_inserted / total_records) * 100
                batch_time = time.time() - batch_start_time
                
                # Print progress
                print(f"{batch_num:<10} {len(batch):<10} {progress_pct:.1f}% ({total_inserted:,}/{total_records:,}) {batch_time:.2f}s")
                
                batch_num += 1
            
            elapsed_time = time.time() - start_time
            
            # Print final results
            print("\n" + "=" * 80)
            print("✅ MONGODB INSERTION SUMMARY")
            print("=" * 80)
            print(f"• Total Records Inserted: {total_inserted:,}")
            print(f"• Total Time: {elapsed_time:.2f} seconds")
            print(f"• Average Insert Rate: {total_inserted/elapsed_time:.2f} records/second")
            print("=" * 80)
            
            return total_inserted
            
        except PyMongoError as e:
            print("\n" + "=" * 80)
            print("❌ ERROR: MongoDB Insertion Failed")
            print(f"• Error details: {str(e)}")
            print("=" * 80)
            return 0

    def display_example_record(self, records_df):
        """
        Display sample records in a nicely formatted way.
        
        Args:
            records_df (DataFrame): DataFrame containing records to display
        """
        if records_df is None or records_df.rdd.isEmpty():
            print("❌ No records available to display")
            return
            
        # Take a sample of records to display
        sample_size = 3
        print(f"\n📝 SAMPLE RECORDS (Showing {sample_size} of {records_df.count():,} records)")
        
        # For each record, format and display
        for i, row in enumerate(records_df.take(sample_size)):
            print("\n" + "=" * 80)
            print(f"📄 SAMPLE RECORD #{i+1}")
            print("=" * 80)
            
            # Convert row to dictionary and display each field
            row_dict = row.asDict()
            for key, value in row_dict.items():
                if isinstance(value, list):
                    formatted_value = ", ".join(map(str, value)) if value else "[]"
                elif isinstance(value, (dict, set)):
                    formatted_value = str(value)
                else:
                    formatted_value = str(value)
                    
                # Truncate long values
                if len(formatted_value) > 100:
                    formatted_value = formatted_value[:97] + "..."
                    
                print(f"• {key}: {formatted_value}")
            print("=" * 80)

    def delete_all_documents(self):
        """
        Delete all documents in the collection.
        
        Returns:
            int: Number of documents deleted
        """
        try:
            # Confirm with the user
            print("\n" + "=" * 80)
            print("⚠️  WARNING: This will delete ALL documents in the collection!")
            print("=" * 80)
            confirm = input("Are you sure you want to proceed? (yes/no): ")
            
            if confirm.lower() not in ["yes", "y"]:
                print("❌ Deletion cancelled.")
                return 0
                
            # Count documents before deletion
            initial_count = self.collection.count_documents({})
            
            # Delete documents
            start_time = time.time()
            result = self.collection.delete_many({})
            elapsed_time = time.time() - start_time
            
            # Print results
            print("\n" + "=" * 80)
            print("✅ MONGODB COLLECTION PURGE RESULTS")
            print("=" * 80)
            print(f"• Initial Document Count: {initial_count:,}")
            print(f"• Deleted Documents: {result.deleted_count:,}")
            print(f"• Deletion Time: {elapsed_time:.2f} seconds")
            print("=" * 80)
            
            return result.deleted_count
            
        except PyMongoError as e:
            print("\n" + "=" * 80)
            print("❌ ERROR: Failed to delete documents")
            print(f"• Error details: {str(e)}")
            print("=" * 80)
            return 0

    def get_collection_stats(self):
        """
        Display statistics about the MongoDB collection using PySpark for analysis.
        """
        try:
            # Get basic stats
            doc_count = self.collection.count_documents({})
            
            # Print collection stats
            print("\n" + "=" * 80)
            print("📊 COLLECTION STATISTICS")
            print("=" * 80)
            print(f"• Total Documents: {doc_count:,}")
            print(f"• Database: {self.database.name}")
            print(f"• Collection: {self.collection.name}")
            print("=" * 80)
            
            # If there are documents, fetch more stats using MongoDB aggregation
            if doc_count > 0:
                # Get sentiment distribution
                pipeline = [
                    {"$group": {"_id": "$sentiment_label", "count": {"$sum": 1}}},
                    {"$sort": {"count": -1}}
                ]
                sentiment_dist = list(self.collection.aggregate(pipeline))
                
                # Get top sources
                pipeline = [
                    {"$group": {"_id": "$source", "count": {"$sum": 1}}},
                    {"$sort": {"count": -1}},
                    {"$limit": 5}
                ]
                top_sources = list(self.collection.aggregate(pipeline))
                
                # Print sentiment distribution
                print("\n" + "=" * 80)
                print("📊 SENTIMENT DISTRIBUTION")
                print("=" * 80)
                print(f"{'SENTIMENT':<15} {'COUNT':<10} {'PERCENTAGE':<10}")
                print("-" * 80)
                
                for item in sentiment_dist:
                    percentage = (item["count"] / doc_count) * 100 if doc_count else 0
                    print(f"{str(item['_id']):<15} {item['count']:<10,} {percentage:.2f}%")
                
                # Print top sources
                print("\n" + "=" * 80)
                print("📊 TOP 5 SOURCES")
                print("=" * 80)
                print(f"{'SOURCE':<30} {'COUNT':<10} {'PERCENTAGE':<10}")
                print("-" * 80)
                
                for item in top_sources:
                    percentage = (item["count"] / doc_count) * 100 if doc_count else 0
                    source_name = str(item["_id"])
                    if len(source_name) > 27:
                        source_name = source_name[:24] + "..."
                    print(f"{source_name:<30} {item['count']:<10,} {percentage:.2f}%")
            
        except PyMongoError as e:
            print("\n" + "=" * 80)
            print("❌ ERROR: Failed to retrieve statistics")
            print(f"• Error details: {str(e)}")
            print("=" * 80)