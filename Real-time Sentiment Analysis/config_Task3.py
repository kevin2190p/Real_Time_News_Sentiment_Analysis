
# API settings
NEWS_API_KEY = "41db74da891e480c9384a475decd3206"  
NEWS_API_URL = "https://newsapi.org/v2/everything"
SEARCH_QUERY = "Petronas" 
LANGUAGE = "en"
SORT_BY = "popularity"

# Kafka settings
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "news-articles"

# Spark settings
APP_NAME = "Real-time Sentiment Analysis"

# Final column structure for enriched data
FINAL_COLUMN_STRUCTURE = [
    "publishedAt", "url", "cleaned_title", "cleaned_description", "source", 
    "source_domain", "category", "word_count", "people_mentioned", 
    "organizations_mentioned", "locations_mentioned", "project_names",
    "financial_figures", "dates_mentioned", "topic_label",
    "sentence", "Sentiment_Result"
]

SENTIMENT_MODEL_PATH = "sentiment_pipeline_spark"
RAW_OUTPUT_PATH = "/user/student/Final_Result/Final5"
ENRICHED_OUTPUT_PATH = "/user/student/Final_Result/Final13"