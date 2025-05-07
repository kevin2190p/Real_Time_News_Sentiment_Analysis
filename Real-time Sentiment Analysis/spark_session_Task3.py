from pyspark.sql import SparkSession
import logging
import warnings

class SparkSessionManager:
    """Creates and manages a SparkSession with required configurations."""
    
    @staticmethod
    def create_spark_session(app_name="Real-time Sentiment Analysis"):
        """
        Creates and returns a SparkSession with the required configurations.
        
        Args:
            app_name (str): Name of the Spark application
            
        Returns:
            SparkSession: Configured Spark session
        """
        logger = logging.getLogger(__name__)
        logger.info("Initializing Spark session...")
        
        # Suppress specific Kafka warnings
        logging.getLogger("org.apache.spark.sql.kafka.KafkaDataConsumer").setLevel(logging.ERROR)
        logging.getLogger("org.apache.kafka").setLevel(logging.ERROR)
        
        # Combine the packages from SPARK_PACKAGES with the Kafka package
        combined_packages = ",".join([
            "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1",  # Spark Kafka connector
            "org.apache.kafka:kafka-clients:2.8.1",             # Kafka client
            "org.apache.spark:spark-streaming-kafka-0-10_2.13:3.5.1",  # Kafka Streaming connector
            "org.apache.spark:spark-token-provider-kafka-0-10_2.13:3.5.1",  # Kafka token provider
            "org.apache.commons:commons-pool2:2.11.0"  # Apache Commons Pool 2
        ])

      
        spark = SparkSession.builder \
                .appName(app_name) \
                .config("spark.jars.packages", combined_packages) \
                .config("spark.sql.streaming.metricsEnabled", "false") \
                .config("spark.sql.shuffle.partitions", "10") \
                .getOrCreate()
            
       
        spark.sparkContext.setLogLevel("ERROR") 
        
        
        conf = spark.sparkContext._jsc.hadoopConfiguration()
        conf.set("mapreduce.job.log4j.hierarchy.append", "false")
        
        
        warnings.filterwarnings("ignore", message=".*KafkaDataConsumer.*")
        
        logger.info("Spark session initialized successfully")
        return spark