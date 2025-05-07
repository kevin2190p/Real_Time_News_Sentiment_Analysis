import requests
import json
import re
import sys
import os

from typing import List, Dict, Optional, Any
from bs4 import BeautifulSoup
from kafka import KafkaProducer, KafkaAdminClient, KafkaConsumer
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from pyspark import SparkContext
from langdetect import detect, LangDetectException

class ArticleScraper:
    """
    Handles scraping and cleaning logic for a single article URL.
    Static method design allows usage within Spark transformations without
    serializing the entire class instance.
    """
    @staticmethod
    def scrape_and_clean(url: str, config: Dict[str, Any]) -> List[Dict[str, str]]:
        """
        Fetches, parses, and cleans content from a single URL.
        Returns a list containing zero or one dictionary: [{'title': ..., 'url': ..., 'content': ...}]
        Args:
            url: The URL to scrape.
            config: Dictionary containing scraping parameters ('timeout', 'user_agent', keyword lists, etc.).
        """
        timeout = config.get('timeout', 30)
        user_agent = config.get('user_agent', 'Mozilla/5.0')
        faq_keywords = config.get('faq_keywords', [])
        non_article_keywords = config.get('non_article_keywords', [])
        non_article_statements = config.get('non_article_statements', [])
        min_content_words = config.get('min_content_words', 10)

        try:
            response = requests.get(url, timeout=timeout, headers={'User-Agent': user_agent})

            if response.status_code in [403, 404, 429, 401, 405] or response.status_code >= 500:
                return []

            response.raise_for_status()

            if 'text/html' not in response.headers.get('Content-Type', '').lower():
                return []

            soup = BeautifulSoup(response.text, 'html.parser')
            title = soup.title.get_text(strip=True) if soup.title else "No Title Found"

            # --- HTML Cleaning ---
            selectors_to_remove = [
                'footer', 'aside', 'nav', 'form', 'header', 'script', 'style', 'noscript',
                'div.subscription', 'div.newsletter', 'div.comments', 'div.related-articles', 'div.advertisement',
                'div.popup', 'div.banner', 'div.sponsored', 'div.social-media', 'div.more-articles', 'div.alerts',
                'section.subscription', 'section.newsletter', 'section.comments', 'section.related-articles', 'section.advertisement',
                'section.popup', 'section.banner', 'section.sponsored', 'section.social-media', 'section.more-articles', 'section.alerts',
                'span.subscription', 'span.newsletter', 'span.comments', 'span.related-articles', 'span.advertisement',
                'span.popup', 'span.banner', 'span.sponsored', 'span.social-media', 'span.more-articles', 'span.alerts',
                'div.topic', 'div.acknowledgment', 'div.external-source', 'div.time-zone', 'div.multilingual', 'div.search',
                'div.manage-alerts', 'div.article-commenting', 'div.breaking-news', 'div.article-comments', 'div.affiliate-links',
            ]
            for selector in selectors_to_remove:
                for element in soup.select(selector):
                    element.decompose()

            tags_to_remove = [
                # Multimedia tags
                'img', 'picture', 'figure', 'figcaption',
                'video', 'audio', 'source', 'track',
                'iframe', 'embed', 'object',
                # Other non-paragraph content or structural elements often irrelevant for text
                'h1', 'h2', 'h3', 'h4', 'h5', 'h6',
                'a',
                'ul', 'ol', 'li',
                'table', 'thead', 'tbody', 'tr', 'th', 'td',
                'button', 'svg', 'canvas'
            ]
            for tag_name in tags_to_remove:
                for tag in soup.find_all(tag_name):
                    tag.decompose()

            # --- Text Extraction & Cleaning ---
            paragraphs = soup.find_all('p')
            page_text = "\n".join([para.get_text(separator=' ', strip=True) for para in paragraphs if para.get_text(strip=True)])

            if not page_text: return []

            try:
                detected_language = detect(page_text)
                if detected_language != 'en':
                    return []
            except LangDetectException:
                return []

            for keyword in non_article_keywords:
                page_text = re.sub(r'[^.?!]*?\b' + re.escape(keyword) + r'\b[^.?!]*?[.?!]', '', page_text, flags=re.IGNORECASE | re.DOTALL)
                page_text = re.sub(r'^\s*\b' + re.escape(keyword) + r'\b[^.?!]*?[.?!]', '', page_text, flags=re.IGNORECASE | re.DOTALL | re.MULTILINE)
            for statement in non_article_statements: page_text = page_text.replace(statement, '')
            for faq in faq_keywords: page_text = re.sub(r'\b' + re.escape(faq) + r'\b', '', page_text, flags=re.IGNORECASE)
            page_text = re.sub(r'(\b(©|All Rights Reserved|Privacy Policy|Terms and Conditions|Cookie Policy|Disclaimer)\b)', '', page_text, flags=re.IGNORECASE)
            page_text = re.sub(r'[^\x00-\x7F]+', '', page_text)
            page_text = re.sub(r'\s+', ' ', page_text).strip()

            # Remove non-English words (keeping only ASCII characters and spaces)
            page_text = re.sub(r'[^\x00-\x7F\s]+', '', page_text).strip()
            page_text = re.sub(r'\s+', ' ', page_text).strip()

            # --- Final Validation ---
            if not page_text or len(page_text.split()) < min_content_words:
                return []

            return [{'title': title, 'url': url, 'content': page_text}]

        except requests.exceptions.Timeout: return []
        except requests.exceptions.RequestException: return []
        except Exception as e:
            return []

def consume_messages(topic_name: str, bootstrap_servers: str, group_id: str = 'my-group'):
    try:
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id=group_id,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')))

        print(f"Consumer started for topic '{topic_name}'...")
        for message in consumer:
            print(f"Received message: Partition={message.partition}, Offset={message.offset}")
            print(f"Key={message.key}")
            print(f"Value={message.value}")

    except NoBrokersAvailable:
        print(f"Error: Could not connect to Kafka brokers at {bootstrap_servers}")
    except json.JSONDecodeError:
        print("Error: Could not decode JSON message from Kafka.")
    except KeyboardInterrupt:
        print("Consumer stopped.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if 'consumer' in locals() and consumer:
            consumer.close()
            print("Consumer closed.")