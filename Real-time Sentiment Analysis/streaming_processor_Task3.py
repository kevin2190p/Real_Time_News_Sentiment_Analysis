from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, lit, explode
import pyspark.sql.functions as F
import logging
import time
from schema_Task3 import SchemaDefinitions
from sentiment_predictor_Task3 import SentimentPredictor
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

class StreamingProcessor:
    """Processes streaming data from Kafka and performs sentiment analysis."""
    
    def __init__(self, spark, kafka_bootstrap_servers, kafka_topic, sentiment_model_path):
        """
        Initialize the streaming processor.
        
        Args:
            spark (SparkSession): The Spark session
            kafka_bootstrap_servers (str): Kafka bootstrap servers
            kafka_topic (str): Kafka topic to subscribe to
            sentiment_model_path (str): Path to the sentiment model file
        """
        self.spark = spark
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic = kafka_topic
        self.kafka_schema = SchemaDefinitions.get_kafka_schema()
        self.final_results = []
        self.logger = logging.getLogger(__name__)
        
        # Initialize the sentiment predictor
        self.sentiment_predictor = SentimentPredictor(spark, sentiment_model_path)
    
    def create_kafka_stream(self):
        """
        Create a streaming DataFrame from Kafka.
        
        Returns:
            DataFrame: Streaming DataFrame from Kafka
        """
        return self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", self.kafka_topic) \
            .option("startingOffsets", "latest") \
            .load()
    
    def create_streaming_dataframe(self, df_kafka, clean_text_udf, split_sentences_udf):
        """
        Transform raw Kafka stream into a structured DataFrame with exploded sentences.
        
        Args:
            df_kafka (DataFrame): Raw Kafka DataFrame
            clean_text_udf: UDF for cleaning text
            split_sentences_udf: UDF for splitting text into sentences
            
        Returns:
            DataFrame: Transformed DataFrame with exploded sentences
        """
        # Cast the raw Kafka message value to string
        df_raw = df_kafka.selectExpr("CAST(value AS STRING) as raw_value", "timestamp")
        
        # Parse the JSON data using the provided schema
        df_parsed = df_raw.select(from_json("raw_value", self.kafka_schema).alias("data"), "timestamp")
        
        # Flatten the JSON struct into individual columns
        df_final = df_parsed.select("data.*", "timestamp")
        
        # Filter out rows where 'title' or 'content' are null
        df_filtered = df_final.filter(col('title').isNotNull() & col('content').isNotNull())
        
        # Clean text fields via the provided UDFs
        cleaned_df = df_filtered \
            .withColumn("cleaned_title", clean_text_udf(col("title"))) \
            .withColumn("cleaned_description", clean_text_udf(col("description"))) \
            .withColumn("cleaned_content", clean_text_udf(col("content")))
        
        # Combine text fields into one field for analysis.
        combined_df = cleaned_df.withColumn("combined_text", 
                    F.when((col("cleaned_content").isNotNull()) & (col("cleaned_content") != ""),
                           col("cleaned_content"))
                     .when((col("cleaned_description").isNotNull()) & (col("cleaned_description") != ""),
                           F.concat(col("cleaned_title"), lit(". "), col("cleaned_description")))
                     .otherwise(col("cleaned_title")))
        
        # Split the cleaned content into sentences using the provided UDF
        df_sentences = combined_df.withColumn("sentences", split_sentences_udf(col("cleaned_content")))
        
        # Explode the sentences array so that each sentence gets its own row
        df_exploded = df_sentences.select(
            "publishedAt", 
            "cleaned_title",
            "cleaned_content",
            "cleaned_description",
            "url",
            F.explode(col("sentences")).alias("sentence")
        )
        
        return df_exploded
    
    def process_batch(self, df, epoch_id):
        """
        Process each batch of streaming data using distributed operations.
        
        Args:
            df (DataFrame): The batch DataFrame
            epoch_id (int): The epoch ID
        """
        try:
            print(f"\nProcessing batch {epoch_id}")
            
            # Rename the column to match model input expectation
       
            df_for_model = df.withColumnRenamed("sentence", "processed_sentence")
            
            # Apply the sentiment model in one distributed transformation
            predictions_df = self.sentiment_predictor.model.transform(df_for_model)

            map_sentiment_udf = udf(lambda score: int(score) - 3 if score is not None else None, IntegerType())

             # Apply mapping directly in a single select operation 
            Mapped_df = predictions_df.select(
                "publishedAt", 
                "cleaned_title", 
                "cleaned_content", 
                "cleaned_description",
                "url", 
                "processed_sentence",
                map_sentiment_udf(col("prediction")).alias("prediction")
            )
           
            Mapped_df.cache()
            Mapped_df.show(truncate=True)
            
            
            batch_results = Mapped_df.rdd.map(lambda row: (
                row.publishedAt,
                row.cleaned_title,
                row.cleaned_content,
                row.cleaned_description,
                row.url,
                row.processed_sentence,
                int(row.prediction)
            )).collect()
            
            if batch_results:
                self.final_results.extend(batch_results)
                
        except Exception as e:
            print(f"Error processing batch {epoch_id}: {str(e)}")
    
    def start_streaming(self, clean_text_udf, split_sentences_udf, news_fetcher):
        """
        Start the streaming query and continuously process data.
        
        Args:
            clean_text_udf: UDF for cleaning text.
            split_sentences_udf: UDF for splitting text into sentences.
            news_fetcher: News fetcher instance used for external control.
        """
        try:
            # Create Kafka stream
            df_kafka = self.create_kafka_stream()
            
            # Transform raw Kafka stream into structured and exploded sentence DataFrame
            df_exploded = self.create_streaming_dataframe(df_kafka, clean_text_udf, split_sentences_udf)
            
            # Use foreachBatch to handle each micro-batch in a distributed manner
            query = df_exploded.writeStream \
                .foreachBatch(self.process_batch) \
                .outputMode("append") \
                .start()

            # Monitor the query periodically; stop if news_fetcher signals to stop
            while query.isActive:
                time.sleep(10)  # Check every 10 seconds
                if not news_fetcher.running:
                    print("Fetcher is not running.")
                    self.logger.info("Fetcher has stopped; stopping streaming query")
                    time.sleep(30)
                    query.stop()
                    break
            
            query.awaitTermination()
            
        except Exception as e:
            print(f"Error in streaming: {str(e)}")
    
    def get_results_dataframe(self):
        """
        Get the consolidated DataFrame of processed results.
        
        Returns:
            DataFrame: A DataFrame with all processed results, coalesced into a single partition.
        """
        if self.final_results:
            result_schema = SchemaDefinitions.get_result_schema()
            result_df = self.spark.createDataFrame(self.final_results, schema=result_schema)
            return result_df.coalesce(1)
        return None