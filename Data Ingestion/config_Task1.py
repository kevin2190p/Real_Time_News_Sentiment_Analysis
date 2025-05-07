import os

CONFIG = {
    "spark": {
        "appName": "NewsPipeline_OOP_Distributed",
        "logLevel": "WARN", 
        "showMaxFields": 100,
    },
    "newsApi": {
        "apiKey": os.environ.get('NEWSAPI_KEY', 'a38a8cf3d941413997ab8b6fdd5d1cc4'), 
        "query": "Petronas",
        "url_template": 'https://newsapi.org/v2/everything?q={query}&sortBy=popularity&language=en&apiKey={apiKey}',
    },
    "kafka": {
        "bootstrap_servers": ['localhost:9092'], 
        "topic_articles": 'news-articles',
        "topic_content": 'news-articles-content',
        "producer_retries": 3,
        "topic_partitions": 1, 
        "topic_replication": 1, 
    },
    "scraping": {
        "timeout": 30,
        "user_agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        "min_content_words": 15,
        "faq_keywords": [
            "faq", "frequently asked questions", "how to", "questions", "help", "contact us", "support", "terms and conditions",
            "privacy policy", "cookie policy", "all rights reserved", "disclaimer", "sitemap", "legal", "copyright"
        ],
        "non_article_keywords": [
             "subscription", "subscribe", "comment", "comments", "create a display name", "Follow Al Jazeera English",
             "Sponsored", "edited by", "Sign up", "name", "email", "website", "news", "offer",
             "Email address", "Follow", "info", "Your bid", "proceed", "inbox", "receive", "Thank you for your report!",
             "Your daily digest", "Search", "Review", "Reviews", "Car Launches", "Driven Communications Sdn. Bhd.", "200801035597 (836938-P)",
             "Follow", "Email address", "Sign up", "For more of the latest", "subscribing", "2025 Hearst Magazines, Inc. .",
             "Connect", "enjoy", "love", "Best", "The Associated Press", "NBCUniversal Media, LLC",
             "Reporting by", "Contact", "ResearchAndMarkets.com", "Advertisement", "thank you", "Your daily digest of everything happening on the site. 2025 Bring a Trailer Media, LLC. .",
             "The materials provided on this Web site are for informational", "Cookies", "Connect With Us", "Back to top",
             "Comments have to be in English", "We have migrated to a new commenting platform", "Vuukle",
             "Patriots membership", "Become a Daily Caller Patriot today", "KicksOnFire.com", "F1technical",
             "Facebook", "Account", "Mediacorp 2025", "TouringPlans.com", "copyright", "Robb Report tote bag", "All below"
        ],
        "non_article_statements": [
             'Search the news', 'Personalise the news', 'stay in the know',
             'Emergency', 'Backstory', 'Newsletters', '中文新闻',
             'BERITA BAHASA INDONESIA', 'TOK PISIN'
        ],
    },
    "output": {
        "parquet_path": "hdfs:///user/student/filtered_articles_spark_oop_parquet",
        "output_coalesce": None,
        "output_mode": "overwrite",
    },
}