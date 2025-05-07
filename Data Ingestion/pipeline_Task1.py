import sys
import news_fetcher
import json
import logging

from typing import List, Dict, Optional, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from pyspark import SparkContext
from kafka_handler_Task1 import KafkaHandler
from news_fetcher_Task1 import NewsAPIFetcher
from article_scraper_Task1 import ArticleScraper

from config_Task1 import CONFIG
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable, KafkaError

logging.getLogger('kafka.coordinator').setLevel(logging.CRITICAL)

class NewsPipeline:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.spark: Optional[SparkSession] = None
        self.sc: Optional[SparkContext] = None
        self.kafka_handler = KafkaHandler(
            bootstrap_servers=config['kafka']['bootstrap_servers'],
            retries=config['kafka']['producer_retries']
        )
        self.news_fetcher = NewsAPIFetcher(
            api_key=config['newsApi']['apiKey'],
            query=config['newsApi']['query'],
            kafka_bootstrap_servers=config['kafka']['bootstrap_servers'],
            kafka_topic=config['kafka']['topic_articles']
        )
        print("NewsPipeline initialized.")

    def _start_spark(self) -> None:
        if self.spark:
            print("SparkSession already started.")
            return
        try:
            self.spark = SparkSession.builder \
                .appName(self.config['spark']['appName']) \
                .config("spark.sql.debug.maxToStringFields", self.config['spark']['showMaxFields']) \
                .getOrCreate()
            self.sc = self.spark.sparkContext
            self.sc.setLogLevel(self.config['spark']['logLevel'])
            print(f"SparkSession '{self.config['spark']['appName']}' started.")
        except Exception as e:
            print(f"FATAL: Error initializing SparkSession: {e}", file=sys.stderr)
            raise

    def _stop_spark(self) -> None:
        if self.spark:
            print("Stopping SparkSession...")
            self.spark.stop()
            self.spark = None
            self.sc = None
            print("SparkSession stopped.")

    def _prepare_kafka_topics(self) -> None:
        print("\n--- Preparing Kafka Topics ---")
        try:
            self.kafka_handler.create_topic_if_not_exists(
                topic_name=self.config['kafka']['topic_articles'],
                num_partitions=self.config['kafka']['topic_partitions'],
                replication_factor=self.config['kafka']['topic_replication']
            )
            self.kafka_handler.create_topic_if_not_exists(
                topic_name=self.config['kafka']['topic_content'],
                num_partitions=self.config['kafka']['topic_partitions'],
                replication_factor=self.config['kafka']['topic_replication']
            )
        except Exception as e:
            print(f"WARNING: Could not ensure Kafka topics exist (may require manual creation or permissions): {e}", file=sys.stderr)

    def _run_step1_fetch_and_produce(self) -> Optional[List[Dict[str, Any]]]:
        print(f"\n--- Step 1: Fetch News & Produce Basic Info to Kafka ('{self.config['kafka']['topic_articles']}') ---")
        articles = self.news_fetcher.fetch_articles()
        produced_count = 0

        if articles is None:
            print("Step 1 failed: Could not fetch articles from NewsAPI.")
            return None
        if not articles:
            print("Step 1 completed: No articles found by NewsAPI.")
            return []

        valid_articles = [a for a in articles if a.get("url")]
        print(f"Fetched {len(articles)} articles, {len(valid_articles)} have URLs.")
        articles = valid_articles

        if not articles:
            print("Step 1 completed: No articles with valid URLs found.")
            return []

        print("Displaying first 3 fetched articles (Title, Desc, URL, PublishedAt, Source):")
        for i, article in enumerate(articles[:3], start=1):
            source_info = article.get("source", {})
            source_name = source_info.get("name", "N/A") if isinstance(source_info, dict) else "N/A"
            print(f"{i}. Title: {article.get('title', 'N/A')}")
            print(f"  Description: {article.get('description', 'N/A')}")
            print(f"  URL: {article.get('url', 'N/A')}")
            print(f"  PublishedAt: {article.get('publishedAt', 'N/A')}")
            print(f"  Source Name: {source_name}\n")
        if len(articles) > 3:
            print("... (only first 3 articles displayed)")

        producer = self.kafka_handler.get_producer()
        if not producer:
            print("Step 1 WARNING: Could not get Kafka producer. Skipping Kafka production for Step 1.")
        else:
            print(f"Producing basic info for {len(articles)} articles to Kafka topic '{self.config['kafka']['topic_articles']}'...")
            for article in articles:
                source_info = article.get("source", {})
                source_name = source_info.get("name", "N/A") if isinstance(source_info, dict) else "N/A"

                article_info = {
                    "title": article.get("title", "N/A"),
                    "description": article.get("description", "N/A"),
                    "url": article.get("url"),
                    "publishedAt": article.get("publishedAt"),
                    "source_name": source_name
                }
                if self.kafka_handler.send_message(producer, self.config['kafka']['topic_articles'], article_info):
                    produced_count += 1

            self.kafka_handler.flush_producer(producer)
            self.kafka_handler.close_producer(producer)
            print(f"Step 1 Kafka Finished: Produced {produced_count} messages to '{self.config['kafka']['topic_articles']}'.")

        print(f"Step 1 Finished. Returning {len(articles)} fetched articles with valid URLs.\n")
        return articles

    def _run_step2_scrape_parallel(self, articles_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        print("\n--- Step 2: Scrape & Clean Article Content (Parallel Spark Job) ---")
        if not articles_data:
            print("Step 2 Skipped: No valid articles received from Step 1.")
            return []
        if not self.sc:
            print("Step 2 Failed: SparkContext not available.")
            raise RuntimeError("SparkContext not initialized before running Step 2.")

        urls_to_scrape = [article["url"] for article in articles_data]
        print(f"Extracted {len(urls_to_scrape)} URLs for scraping.")
        if not urls_to_scrape:
            print("Step 2 Finished: No URLs to scrape.")
            return []

        broadcast_config = self.sc.broadcast(self.config['scraping'])

        url_rdd = self.sc.parallelize(urls_to_scrape, numSlices=len(urls_to_scrape))
        cleaned_content_rdd = url_rdd.flatMap(
            lambda url: ArticleScraper.scrape_and_clean(url, broadcast_config.value)
        )

        print("Collecting cleaned content from Spark workers...")
        try:
            cleaned_content_list = cleaned_content_rdd.collect()
            print(f"Step 2 Finished: Collected scraped content for {len(cleaned_content_list)} articles.\n")
            return cleaned_content_list
        except Exception as e:
            print(f"ERROR during Spark collect operation in Step 2: {e}", file=sys.stderr)
            return []

    def _run_step3_produce_cleaned(self, merged_data: List[Dict[str, Any]]) -> None:
        print(f"\n--- Step 3: Produce Merged Content to Kafka ('{self.config['kafka']['topic_content']}') ---")
        produced_count = 0
        if not merged_data:
            print("Step 3 Skipped: No merged data available (perhaps scraping failed or no initial articles).")
            return

        producer = self.kafka_handler.get_producer()
        if not producer:
            print("Step 3 Failed: Could not get Kafka producer.")
            return

        print(f"Producing merged data for {len(merged_data)} articles to Kafka topic '{self.config['kafka']['topic_content']}'...")
        for item in merged_data:
            if self.kafka_handler.send_message(producer, self.config['kafka']['topic_content'], item):
                produced_count += 1

        self.kafka_handler.flush_producer(producer)
        self.kafka_handler.close_producer(producer)
        print(f"Step 3 Finished: Produced {produced_count} messages to '{self.config['kafka']['topic_content']}'.\n")

    def _run_step4_create_dataframe(self, merged_data: List[Dict[str, Any]]) -> None:
        print("\n--- Step 4: Create Spark DataFrame & Save Output ---")
        if not merged_data:
            print("Step 4 Skipped: No merged data available.")
            return
        if not self.spark:
            print("Step 4 Failed: SparkSession not available.")
            raise RuntimeError("SparkSession not initialized before running Step 4.")

        try:
            schema = StructType([
                StructField("title", StringType(), True),
                StructField("url", StringType(), True),
                StructField("content", StringType(), True),
                StructField("publishedAt", StringType(), True),
                StructField("source_name", StringType(), True)
            ])

            print("Creating Spark DataFrame from merged data...")
            df: DataFrame = self.spark.createDataFrame(merged_data, schema=schema)

            print("DataFrame created successfully.")

            output_path = self.config['output']['parquet_path']
            coalesce_partitions = self.config['output']['output_coalesce']
            write_mode = self.config['output']['output_mode']
            print(f"Attempting to save DataFrame to: {output_path} (Mode: {write_mode}, Format: Parquet)")

            df_writer = df.write.mode(write_mode)

            if coalesce_partitions and isinstance(coalesce_partitions, int) and coalesce_partitions > 0:
                print(f"Coalescing DataFrame to {coalesce_partitions} partition(s) before writing.")
                df = df.coalesce(coalesce_partitions)
                df_writer = df.write.mode(write_mode)

            df_writer.parquet(output_path)

            print(f"DataFrame successfully saved as Parquet to: {output_path}")
            print(f"Step 4 Finished.\n")

        except Exception as e:
            print(f"ERROR during DataFrame creation or saving in Step 4: {e}", file=sys.stderr)

    def _consume_kafka_messages(self) -> None:
        topic_name = self.config['kafka']['topic_content']
        bootstrap_servers = self.config['kafka']['bootstrap_servers']
        group_id = self.config.get('kafka', {}).get('consumer_group_id', 'news_pipeline_consumer')
        consumer_timeout_ms = 5000
        received_count = 0

        try:
            consumer = KafkaConsumer(
                topic_name,
                bootstrap_servers=bootstrap_servers,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id=group_id,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=consumer_timeout_ms
            )

            print(f"\n--- Kafka Consumer Started for topic '{topic_name}' (Timeout: {consumer_timeout_ms} ms) ---")
            messages_received = False
            displayed_count = 0
            for record in consumer:
                if displayed_count < 3:
                    print(f"Received message: Partition={record.partition}, Offset={record.offset}")
                    print(f"Key={record.key}")
                    print(f"Value={record.value.get('title', 'N/A')}")
                    print("Processing consumed message...")
                    print("-" * 20)
                    displayed_count += 1
                received_count += 1

            if received_count > 0:
                print(f"Kafka Consumer: Received a total of {received_count} articles.")
            else:
                print("Kafka Consumer: No messages were available in the topic.")

        except NoBrokersAvailable:
            print(f"Error: Could not connect to Kafka brokers at {bootstrap_servers}")
        except json.JSONDecodeError:
            print("Error: Could not decode JSON message from Kafka.")
        except KafkaError as e:
            print(f"Kafka Consumer Error: {e}")
        except KeyboardInterrupt:
            print("Kafka consumer stopped by user.")
        finally:
            if 'consumer' in locals() and consumer:
                consumer.close()
                print("Kafka consumer closed.")

    def run(self) -> None:
        print("Starting News Processing Pipeline...")
        try:
            self._start_spark()
            self._prepare_kafka_topics()
            step1_articles = self._run_step1_fetch_and_produce()
            articles_for_step2 = step1_articles if isinstance(step1_articles, list) else []
            step2_cleaned_data = self._run_step2_scrape_parallel(articles_for_step2)

            print("\n--- Merging Original Article Metadata with Scraped Content ---")
            if not articles_for_step2 or not step2_cleaned_data:
                print("Merge Skipped: Missing original articles or scraped data.")
                merged_data = []
            else:
                original_articles_map = {a['url']: a for a in articles_for_step2 if a.get('url')}
                scraped_content_map = {c['url']: c for c in step2_cleaned_data if c.get('url')}

                merged_data = []
                processed_urls = set()

                for url, scraped_info in scraped_content_map.items():
                    if url in original_articles_map and url not in processed_urls:
                        original_info = original_articles_map[url]
                        source_info = original_info.get("source", {})
                        source_name = source_info.get("name", "N/A") if isinstance(source_info, dict) else "N/A"

                        merged_item = {
                            "title": original_info.get("title", scraped_info.get("title", "N/A")),
                            "url": url,
                            "content": scraped_info.get("content"),
                            "publishedAt": original_info.get("publishedAt"),
                            "source_name": source_name
                        }

                        if merged_item["content"]:
                            merged_data.append(merged_item)
                            processed_urls.add(url)
                        else:
                            print(f"INFO: Skipping article (URL: {url}) due to missing scraped content after merge.")
                print(f"Successfully merged metadata with scraped content for {len(merged_data)} articles.")

            self._run_step3_produce_cleaned(merged_data)
            self._consume_kafka_messages()
            self._run_step4_create_dataframe(merged_data)

            print("\nNews Processing Pipeline Finished Successfully.")

        except Exception as e:
            print(f"FATAL ERROR: An error occurred during pipeline execution: {e}", file=sys.stderr)

        finally:
            self._stop_spark()