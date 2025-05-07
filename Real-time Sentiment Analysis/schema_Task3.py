from pyspark.sql.types import StructType, StructField, StringType, TimestampType, FloatType, ArrayType, IntegerType

class SchemaDefinitions:
    """Defines schemas used in the application."""
    
    @staticmethod
    def get_kafka_schema():
        """
        Returns the schema for Kafka messages.
        
        Returns:
            StructType: Schema for Kafka messages
        """
        return StructType([
            StructField("publishedAt", StringType(), True), 
            StructField("title", StringType(), True),
            StructField("description", StringType(), True),
            StructField("url", StringType(), True),
            StructField("content", StringType(), True)  
        ])
    
    @staticmethod
    def get_result_schema():
        """
        Returns the schema for sentiment analysis results.
        
        Returns:
            StructType: Schema for sentiment results
        """
        return StructType([
            StructField("publishedAt", StringType(), True),
            StructField("cleaned_title", StringType(), True),
            StructField("cleaned_content", StringType(), True),
            StructField("cleaned_description", StringType(), True),
            StructField("url", StringType(), True),
            StructField("sentence", StringType(), True),
            StructField("Sentiment_Result", IntegerType(), True)
        ])