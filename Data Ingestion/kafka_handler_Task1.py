import requests
import json
import re
import sys
import os

from bs4 import BeautifulSoup
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from pyspark import SparkContext
from typing import List, Dict, Optional, Any
from typing import List, Dict, Optional, Any

class KafkaHandler:
    """
    Handles interactions with Kafka, including topic creation and message production.
    Encapsulates Kafka connection details and producer logic.
    """
    def __init__(self, bootstrap_servers: List[str], retries: int = 3):
        self.bootstrap_servers = bootstrap_servers
        self.retries = retries
        print(f"KafkaHandler initialized for servers: {self.bootstrap_servers}")

    def create_topic_if_not_exists(self, topic_name: str, num_partitions: int = 1, replication_factor: int = 1) -> None:
        """Checks if a Kafka topic exists and creates it if not."""
        admin_client = None
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
            existing_topics = admin_client.list_topics()
            if topic_name not in existing_topics:
                print(f"Topic '{topic_name}' not found. Attempting to create...")
                topic = NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
                admin_client.create_topics(new_topics=[topic], validate_only=False)
                print(f"Topic '{topic_name}' created successfully.")
            else:
                print(f"Topic '{topic_name}' already exists.")
        except NoBrokersAvailable:
            print(f"ERROR: Cannot connect to Kafka brokers at {self.bootstrap_servers}. Please ensure Kafka is running.", file=sys.stderr)
            raise
        except Exception as e:
            print(f"ERROR during Kafka topic check/creation for '{topic_name}': {e}", file=sys.stderr)
        finally:
            if admin_client:
                admin_client.close()

    def get_producer(self) -> Optional[KafkaProducer]:
        """Creates and returns a KafkaProducer instance."""
        try:
            producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                retries=self.retries
            )
            print(f"KafkaProducer connected to {self.bootstrap_servers}.")
            return producer
        except NoBrokersAvailable:
            print(f"ERROR: Kafka connection failed for producer. Cannot produce messages.", file=sys.stderr)
            return None
        except Exception as e:
            print(f"ERROR: Failed to initialize KafkaProducer: {e}", file=sys.stderr)
            return None

    @staticmethod
    def send_message(producer: KafkaProducer, topic: str, value: Dict[str, Any]) -> bool:
        """Sends a single message using the provided producer."""
        if not producer: return False
        try:
            producer.send(topic, value=value)
            return True
        except Exception as e:
            print(f"ERROR sending message to topic '{topic}': {e}", file=sys.stderr)
            return False

    @staticmethod
    def flush_producer(producer: Optional[KafkaProducer]) -> None:
        """Flushes the producer to ensure messages are sent."""
        if producer:
            try:
                print("Flushing Kafka producer...")
                producer.flush()
            except Exception as e:
                 print(f"ERROR flushing Kafka producer: {e}", file=sys.stderr)

    @staticmethod
    def close_producer(producer: Optional[KafkaProducer]) -> None:
        """Closes the Kafka producer."""
        if producer:
            try:
                print("Closing Kafka producer.")
                producer.close()
            except Exception as e:
                print(f"ERROR closing Kafka producer: {e}", file=sys.stderr)