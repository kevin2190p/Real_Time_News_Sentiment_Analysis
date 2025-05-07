import json
import time
import threading
import logging
import requests


from text_cleaner.processor.processor import RegexProcessor


from kafka import KafkaProducer
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

NEWS_API_KEY = "41db74da891e480c9384a475decd3206"  
NEWS_API_URL = "https://newsapi.org/v2/everything"
SEARCH_QUERY = "Petronas" 
LANGUAGE = "en"
SORT_BY = "popularity"

class NewsAPIFetcher:
    """
    Class to fetch articles from NewsAPI and send them to Kafka.
    """
   
    
    def __init__(self, api_key, query, kafka_bootstrap_servers, kafka_topic):
        self.api_key = api_key
        self.query = query
        self.kafka_producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.kafka_topic = kafka_topic
        self.last_fetch_time = None
        self.fetch_interval = 300  # 5 minutes in seconds
        self.running = False
        self.thread = None
    
    def fetch_articles(self):
        """
        Fetches articles from NewsAPI based on the query.
        """
        try:
            # Construct the API URL
            url = (f'{NEWS_API_URL}?'
                  f'q={self.query}&'
                  f'sortBy={SORT_BY}&'
                  f'language={LANGUAGE}&'
                  f'apiKey={self.api_key}')
            
            # Add from parameter if we've fetched before to avoid duplicates
            if self.last_fetch_time:
                # Format the time as ISO 8601
                from_time = self.last_fetch_time.strftime('%Y-%m-%dT%H:%M:%S')
                url += f'&from={from_time}'
            
            # Send the GET request and parse the JSON response
            response = requests.get(url)
            data = response.json()
            
            # Update the last fetch time
            self.last_fetch_time = time.time()
            
            # Process the articles
            if "articles" in data:
                return data["articles"]
            else:
                logger.warning(f"No articles found in API response: {data}")
                return []
        except Exception as e:
            logger.error(f"Error fetching articles from NewsAPI: {str(e)}")
            return []
    
    def process_and_send_articles(self, articles):
        """
        Processes articles and sends them to Kafka.
        """
        sent_count = 0
        for article in articles:
            try:
                # Extract article information
                title = article.get("title", "")
                description = article.get("description", "")
                url = article.get("url", "")
                
                # Skip if title or URL is missing
                if not title or not url:
                    continue
                
                # Fetch and clean the article content
                content = RegexProcessor.fetch_and_clean_article_content(url)
                
                # Prepare the article information
                article_info = {
                    "title": title,
                    "description": description,
                    "url": url,
                    "content": content
                }
                
                # Send the article data to Kafka topic
                self.kafka_producer.send(self.kafka_topic, value=article_info)
                
                logger.info(f"Sent article to Kafka: {title}")
                sent_count += 1
                
            except Exception as e:
                logger.error(f"Error processing article: {str(e)}")
        
        # Flush to ensure all messages are sent
        self.kafka_producer.flush()
        logger.info(f"Total articles sent to Kafka: {sent_count}")
        return sent_count
    
    def fetch_and_send(self):
        """
        Fetches articles and sends them to Kafka.
        """
        articles = self.fetch_articles()
        return self.process_and_send_articles(articles)
    
    def run_continuously(self):
        """
        Runs the fetcher continuously at the specified interval.
        """
        while self.running:
            try:
                self.fetch_and_send()
            except Exception as e:
                logger.error(f"Error in continuous fetching: {str(e)}")
            
            # Sleep for the specified interval
            time.sleep(self.fetch_interval)
    
    def start(self):
        """
        Starts the fetcher in a separate thread.
        """
        if not self.running:
            self.running = True
            self.thread = threading.Thread(target=self.run_continuously)
            self.thread.daemon = True
            self.thread.start()
            logger.info("NewsAPI fetcher started")
    
    def stop(self):
        """
        Stops the fetcher.
        """
        if self.running:
            self.running = False
            if self.thread:
                self.thread.join(timeout=10)
            logger.info("NewsAPI fetcher stopped")
