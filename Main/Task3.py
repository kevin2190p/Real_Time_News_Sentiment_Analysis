import logging
import os
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, ArrayType

# Import custom modules
from spark_session_Task3 import SparkSessionManager
from streaming_processor_Task3 import StreamingProcessor
from data_enricher_wrapper_Task3 import DataEnricherWrapper
from config_Task3 import (
    KAFKA_BOOTSTRAP_SERVERS, 
    KAFKA_TOPIC, 
    NEWS_API_KEY, 
    SEARCH_QUERY, 
    APP_NAME,
    SENTIMENT_MODEL_PATH,
    RAW_OUTPUT_PATH,
    ENRICHED_OUTPUT_PATH
)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """Main execution function"""
    try:
        # Create Spark session
        spark = SparkSessionManager.create_spark_session(APP_NAME)
        print("Spark session created successfully")
        
        # Import text processing modules
        from text_cleaner_Task3 import TextCleaner
        from news_fetcher_Task3 import NewsAPIFetcher
        
        # Register UDFs for text processing
        clean_text_udf = udf(TextCleaner.clean_text, StringType())
        split_sentences_udf = udf(TextCleaner.split_into_sentences, ArrayType(StringType()))
        
        print("Text processing functions defined and UDFs registered")
        
        # Initialize the NewsAPI fetcher
        news_fetcher = NewsAPIFetcher(
            api_key=NEWS_API_KEY,
            query=SEARCH_QUERY,
            kafka_bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            kafka_topic=KAFKA_TOPIC
        )
        
        # Create streaming processor
        processor = StreamingProcessor(
            spark=spark,
            kafka_bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            kafka_topic=KAFKA_TOPIC,
            sentiment_model_path=SENTIMENT_MODEL_PATH
        )
        
        # Start news fetcher
        news_fetcher.start()
        
        # Start streaming and perform sentiment analysis
        processor.start_streaming(clean_text_udf, split_sentences_udf, news_fetcher)
        
        # Get results DataFrame
        result_df = processor.get_results_dataframe()
        
        if result_df:
            # Show results
            result_df.show()
            
            # Enrich and save data
            enricher = DataEnricherWrapper(spark)
            enricher.process_data(result_df, ENRICHED_OUTPUT_PATH)
        else:
            print("No results to process")
            
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
    finally:
        # Stop SparkSession 
        if 'spark' in locals():
            spark.stop()
            logger.info("Spark session stopped")

if __name__ == "__main__":
    main()