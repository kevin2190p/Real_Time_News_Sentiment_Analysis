from pymongo import MongoClient
from datetime import datetime
import pandas as pd
from collections import Counter

class Query_and_Report:
    def __init__(self, MONGO_URI, DATABASE_NAME, COLLECTION_NAME):
        """Initialize the MongoDB client and connection to the specific database and collection."""
        self.client = MongoClient(MONGO_URI)
        self.database_name = self.client[DATABASE_NAME]
        self.collection_name = self.database_name[COLLECTION_NAME]

    def get_positive_sentiment_sentences_by_articles(self):
        pipeline = [
            {"$match": {
                "sentiment_label": "positive", 
                "Sentiment_Result": {"$gt": 0} # greataer than 0 (positive sentiment)
            }},
            {"$group": {
                "_id": "$cleaned_title",
                "sentences": {"$push": "$sentence"}, #collects all sentence texts into a list (sentences)
                "positive_count": {"$sum": 1},
                "average_sentiment": {"$avg": "$Sentiment_Result"},
                "sentiment_results": {"$push": "$Sentiment_Result"}
            }},
            {"$sort": {"positive_count": -1}} # sort by positive sentence in decending
        ]
        
        positive_sentiment_sentences_by_articles = self.collection_name.aggregate(pipeline)
    
        print("Positive Sentiment Articles (sorted by positive sentence count):")
        for doc in positive_sentiment_sentences_by_articles:
            print(f"\nTitle: {doc['_id']}")
            print(f"Total Sentences: {len(doc['sentences'])}")
            print(f"Sentiment Label: All Positive")
            
            sentiment_result_1_count = sum(1 for res in doc['sentiment_results'] if res == 1)
            sentiment_result_2_count = sum(1 for res in doc['sentiment_results'] if res == 2)
            print(f"Total Sentiment Result 1: {sentiment_result_1_count}")
            print(f"Total Sentiment Result 2: {sentiment_result_2_count}")
            
            print(f"\n{'#':<5} {'Sentence':<100} {'Sentiment Result':<20}")
            print("="*120)
            
            for idx, (sentence, sentiment_result) in enumerate(zip(doc['sentences'], doc['sentiment_results']), 1):
                print(f"{idx:<5} {sentence:<100} {sentiment_result:<20}")
            
            print(f"\nAverage Sentiment Result: {doc['average_sentiment']}")
            print("=" * 120)
            
        return positive_sentiment_sentences_by_articles

    def get_negative_sentiment_sentences_by_articles(self):
        pipeline = [
            {"$match": {
                "sentiment_label": "negative", 
                "Sentiment_Result": {"$lt": 0} # less than 0 (negative sentiment)
            }},
            {"$group": {
                "_id": "$cleaned_title",
                "sentences": {"$push": "$sentence"},
                "negative_count": {"$sum": 1},
                "average_sentiment": {"$avg": "$Sentiment_Result"},
                "sentiment_results": {"$push": "$Sentiment_Result"}
            }},
            {"$sort": {"negative_count": -1}}
        ]
        
        negative_sentiment_sentences_by_articles = self.collection_name.aggregate(pipeline)
    
        print("Negative Sentiment Articles (sorted by negative sentence count):")
        for doc in negative_sentiment_sentences_by_articles:
            print(f"\nTitle: {doc['_id']}")
            print(f"Total Sentences: {len(doc['sentences'])}")
            print(f"Sentiment Label: All Negative")
            
            sentiment_result_neg1_count = sum(1 for res in doc['sentiment_results'] if res == -1)
            sentiment_result_neg2_count = sum(1 for res in doc['sentiment_results'] if res == -2)
            print(f"Total Sentiment Result -1: {sentiment_result_neg1_count}")
            print(f"Total Sentiment Result -2: {sentiment_result_neg2_count}")
            
            print(f"\n{'#':<5} {'Sentence':<100} {'Sentiment Result':<20}")
            print("="*120)
            
            for idx, (sentence, sentiment_result) in enumerate(zip(doc['sentences'], doc['sentiment_results']), 1):
                print(f"{idx:<5} {sentence:<100} {sentiment_result:<20}")
            
            print(f"\nAverage Sentiment Result: {doc['average_sentiment']}")
            print("=" * 120)
            
        return negative_sentiment_sentences_by_articles

    def find_neutral_or_positive_sentiment_articles_with_location(self, location_1, location_2, n):
        """Find articles with neutral or positive sentiment label and specific locations."""
        pipeline = [
            {"$match": {
                "sentiment_label": {"$in": ["neutral", "positive"]},
                "locations_mentioned": {"$in": [location_1, location_2]}
            }},
            {"$sort": {"publishedAt": -1}},
            {"$limit": n}, # limits to top n results
            {"$project": { # same as select statement 
                "_id": 0,
                "title": "$cleaned_title",
                "cleaned_description": 1,
                "publishedAt": 1,
                "sentiment_label": 1,
                "locations_mentioned": 1,
                "sentence": 1
            }}
        ]
        
        neutral_or_positive_sentiment_articles_with_location = list(self.collection_name.aggregate(pipeline))
        
        print(f"Found {len(neutral_or_positive_sentiment_articles_with_location)} neutral or positive sentiment articles with location '{location_1}' or '{location_2}'")
        for idx, article in enumerate(neutral_or_positive_sentiment_articles_with_location, 1):
            print("=" * 50)
            print(f"\nArticle {idx}:")
            print(f"Title: {article['title']}")
            print(f"Description: {article['cleaned_description']}")        
            print(f"Published At: {article['publishedAt']}")
            print(f"Sentiment Label: {article['sentiment_label']}")
            print(f"Locations Mentioned: {', '.join(article['locations_mentioned'])}")
            print(f"Sentence: {article['sentence']}\n")
    
        return neutral_or_positive_sentiment_articles_with_location

    def get_most_common_categories(self, sentiment_label, top_n):
        pipeline = [
            {"$match": {"sentiment_label": sentiment_label}},
            {"$group": {
                "_id": "$category",
                "count": {"$sum": 1} # count how many times each category appears
            }},
            {"$sort": {"count": -1}},
            {"$limit": top_n}
        ]
        
        most_common_categories = list(self.collection_name.aggregate(pipeline))
        
        print(f"\nTop {top_n} categories associated with {sentiment_label} sentiment:")
        
        df = pd.DataFrame(most_common_categories)
        df.columns = ['Category', 'Count']
        
        print(df.to_string(index=False))
        
        return most_common_categories

    def get_articles_by_published_date(self, start_date, end_date, limit):
        match_conditions = {}
        if start_date and end_date:
            match_conditions = {
                "publishedAt": {"$gte": start_date, "$lte": end_date}
            }
    
        pipeline = [
            {"$match": match_conditions},
            {"$project": {
                "publishedAt": 1,
                "cleaned_title": 1,
                "cleaned_description": 1,
                "sentiment_label": 1,
                "source": 1,
                "category": 1,
                "sentence": 1,
                "url": 1,
                "Sentiment_Result": 1
            }},
            {"$group": {
                "_id": {"title": "$cleaned_title", "description": "$cleaned_description"},
                "sentiment_labels": {"$addToSet": "$sentiment_label"},
                "sentences": {"$push": "$sentence"}, #collects all sentences/sentiments
                "sentiment_results": {"$push": "$Sentiment_Result"},
                "sources": {"$addToSet": "$source"}, # creates a unique list (removes duplicates)
                "categories": {"$addToSet": "$category"},
                "publishedAt": {"$first": "$publishedAt"},
                "url": {"$first": "$url"}
            }},
            {"$addFields": {
                "sentence_count": {"$size": "$sentences"},
                "average_sentiment_result": {"$avg": "$sentiment_results"}
            }},
            {"$sort": {"publishedAt": -1}},
            {"$limit": limit}
        ]
        
        articles_by_published_date = list(self.collection_name.aggregate(pipeline))
        
        if articles_by_published_date:
            print(f"Articles sorted by Published Date:")
            for article in articles_by_published_date:
                print("=" * 50)
                print(f"Published At: {article['publishedAt']}")
                print(f"Title: {article['_id']['title']}")
                print(f"Description: {article['_id']['description']}")
                print(f"Sentiment Labels: {', '.join(article['sentiment_labels'])}")
                print(f"Total Sentences: {article['sentence_count']}")
                print(f"Average Sentiment Result: {article['average_sentiment_result']}")
                print(f"Sources: {', '.join(article['sources'])}")
                print(f"Categories: {', '.join(article['categories'])}")
                print(f"URL: {article['url']}")
                print("=" * 50)
        else:
            print("No articles found based on the given criteria.")
    
        return articles_by_published_date

    def find_mixed_sentiment_articles_with_percentiles(self):
        pipeline = [
            {"$group": {
                "_id": "$cleaned_title",
                "sentiments": {"$addToSet": "$sentiment_label"},
                "count": {"$sum": 1},  # if condition true → 1, else 0
                "positive_count": {"$sum": {"$cond": [{"$eq": ["$sentiment_label", "positive"]}, 1, 0]}},
                "neutral_count": {"$sum": {"$cond": [{"$eq": ["$sentiment_label", "neutral"]}, 1, 0]}},
                "negative_count": {"$sum": {"$cond": [{"$eq": ["$sentiment_label", "negative"]}, 1, 0]}},
                "sources": {"$addToSet": "$source"},
                "urls": {"$addToSet": "$url"},
                "cleaned_description": {"$first": "$cleaned_description"},
                "publishedAt": {"$first": "$publishedAt"}
            }},
            {"$match": {
                "$expr": {"$gt": [{"$size": "$sentiments"}, 1]}
            }},
            {"$sort": {"count": -1}}
        ]
        
        mixed_sentiment_articles = list(self.collection_name.aggregate(pipeline))
        
        print(f"Found {len(mixed_sentiment_articles)} articles with mixed sentiment and percentiles calculated")
        for article in mixed_sentiment_articles:
            positive_count = article['positive_count']
            neutral_count = article['neutral_count']
            negative_count = article['negative_count']
            
            total_count = positive_count + neutral_count + negative_count
            
            if total_count > 0:
                positive_percentage = (positive_count / total_count) * 100
                neutral_percentage = (neutral_count / total_count) * 100
                negative_percentage = (negative_count / total_count) * 100
            else:
                positive_percentage = neutral_percentage = negative_percentage = 0
            
            positive_percentile = round(positive_percentage, 2)
            neutral_percentile = round(neutral_percentage, 2)
            negative_percentile = round(negative_percentage, 2)
    
            print(f"\nTitle: {article['_id']}")
            print(f"Sentiments: {', '.join(article['sentiments'])}")
            print(f"Sentence count: {article['count']}")
            print(f"Positive Sentiment Count: {article['positive_count']}   ({positive_percentile}%)")
            print(f"Neutral Sentiment Count: {article['neutral_count']}    ({neutral_percentile}%)")
            print(f"Negative Sentiment Count: {article['negative_count']}   ({negative_percentile}%)")
            print(f"Description: {article['cleaned_description']}")
            print(f"Sources: {', '.join(article['sources'])}")
            print(f"URLs: {', '.join(article['urls'])}")
            print(f"Published At: {article['publishedAt']}\n")
            print("=" * 50)
    
        return mixed_sentiment_articles

    def find_top_positive_sentiment_articles(self):
        """Find articles with the most positive sentiment and highest number of sentences."""
        pipeline = [
            {"$match": {"sentiment_label": "positive"}},
            {"$group": {
                "_id": "$cleaned_title",
                "positive_count": {"$sum": 1},
                "sentences": {"$push": "$sentence"}
            }},
            {"$sort": {"positive_count": -1}}, #  sorts the documents by the positive_count field in descending order.
            {"$limit": 10}
        ]
            
        top_positive_sentiment_articles = list(self.collection_name.aggregate(pipeline))
            
        print(f"Top Positive Sentiment Articles:")
        for article in top_positive_sentiment_articles:
            print("=" * 50) 
            print(f"\nTitle: {article['_id']}\n")
            print(f"Positive Sentiment Count: {article['positive_count']}\n")
            for idx, sentence in enumerate(article['sentences'], 1):
                print(f"Sentence {idx}: {sentence}")
        
        return top_positive_sentiment_articles

    def find_top_negative_sentiment_articles(self):
        """Find articles with the most negative sentiment (Sentiment_Result <= -2) and highest number of sentences."""
        pipeline = [
            {"$match": {"Sentiment_Result": {"$lte": -2}}},
            {"$group": {
                "_id": "$cleaned_title",
                "negative_count": {"$sum": 1},
                "sentences": {"$push": "$sentence"}
            }},
            {"$sort": {"negative_count": -1}},
            {"$limit": 10}
        ]
            
        top_negative_sentiment_articles = list(self.collection_name.aggregate(pipeline))
        
        print(f"Top Negative Sentiment Articles:")
        for article in top_negative_sentiment_articles:
            print("=" * 50) 
            print(f"\nTitle: {article['_id']}\n")
            print(f"Negative Sentiment Count: {article['negative_count']}\n")
            for idx, sentence in enumerate(article['sentences'], 1):
                print(f"Sentence {idx}: {sentence}")
        
        return top_negative_sentiment_articles

    def search_articles_with_sentences_by_keyword(self, keyword):
        """Search for articles where the title or description contains the specific keyword."""
        pipeline = [
            {"$match": {
                "$or": [
                    {"cleaned_title": {"$regex": keyword, "$options": "i"}},
                    {"cleaned_description": {"$regex": keyword, "$options": "i"}}
                ]
            }},
            {"$group": {
                "_id": "$cleaned_title",
                "sentences": {"$push": "$sentence"},
                "sentiment_labels": {"$addToSet": "$sentiment_label"},
                "positive_count": {"$sum": {"$cond": [{"$eq": ["$sentiment_label", "positive"]}, 1, 0]}},
                "neutral_count": {"$sum": {"$cond": [{"$eq": ["$sentiment_label", "neutral"]}, 1, 0]}},
                "negative_count": {"$sum": {"$cond": [{"$eq": ["$sentiment_label", "negative"]}, 1, 0]}},
                "sources": {"$addToSet": "$source"},
                "urls": {"$addToSet": "$url"}
            }},
            {"$sort": {"positive_count": -1}}
        ]
        
        articles_with_sentences_by_keyword = list(self.collection_name.aggregate(pipeline))
        
        print(f"Articles containing the keyword '{keyword}':")
        for article in articles_with_sentences_by_keyword:
            print("=" * 50)
            print(f"\nTitle: {article['_id']}")
            print(f"Sentiment Labels: {', '.join(article['sentiment_labels'])}")
            print(f"Positive Sentiment Count: {article['positive_count']}")
            print(f"Neutral Sentiment Count: {article['neutral_count']}")
            print(f"Negative Sentiment Count: {article['negative_count']}")
            print(f"Sources: {', '.join(article['sources'])}")
            print(f"URLs: {', '.join(article['urls'])}")
            
            print("\nSentences:")
            for idx, sentence in enumerate(article['sentences'], 1):
                print(f"Sentence {idx}: {sentence}")
            
            print("=" * 50)

    def extract_articles_by_keywords(self, keywords, start_date=None, end_date=None, limit=10):
        """Extract articles containing specific keywords in their content. Allows for filtering by date range."""
        match_conditions = {
            "sentence": {"$regex": "|".join(keywords), "$options": "i"}
        }
        
        if start_date and end_date:
            match_conditions["publishedAt"] = {"$gte": start_date, "$lte": end_date}
        
        pipeline = [
            {"$match": match_conditions},
            {"$group": {
                "_id": "$cleaned_title",
                "sentences": {"$push": "$sentence"},
                "sentiment_label": {"$first": "$sentiment_label"},
                "publishedAt": {"$first": "$publishedAt"},
                "source": {"$first": "$source"},
                "url": {"$first": "$url"}
            }},
            {"$sort": {"publishedAt": -1}},
            {"$limit": limit}
        ]
        
        articles_by_keywords = list(self.collection_name.aggregate(pipeline))
        
        if articles_by_keywords:
            print(f"Articles containing the keywords {', '.join(keywords)}:")
            for doc in articles_by_keywords:
                print("=" * 50)
                print(f"\nTitle: {doc['_id']}")
                print(f"Published At: {doc['publishedAt']}")
                print(f"Sentiment Label: {doc['sentiment_label']}")
                print(f"Source: {doc['source']}")
                print(f"URL: {doc['url']}")
                
                print("\nSentences with the keywords:")
                for idx, sentence in enumerate(doc['sentences'], 1):
                    print(f"Sentence {idx}: {sentence}")
                
                print("=" * 50)
        else:
            print("No articles found for the given keywords.")
    
        return articles_by_keywords

    def get_daily_sentiment_report_with_percentages(self):
        """Generates a daily sentiment report with percentage distribution of each sentiment label."""
        pipeline = [
            {"$group": {
                "_id": {
                    "date": {"$substr": ["$publishedAt", 0, 10]},
                    "title": "$cleaned_title"
                },
                "count": {"$sum": 1},
                "average_sentiment": {"$avg": "$Sentiment_Result"},
                "positive_count": {"$sum": {"$cond": [{"$eq": ["$sentiment_label", "positive"]}, 1, 0]}},
                "neutral_count": {"$sum": {"$cond": [{"$eq": ["$sentiment_label", "neutral"]}, 1, 0]}},
                "negative_count": {"$sum": {"$cond": [{"$eq": ["$sentiment_label", "negative"]}, 1, 0]}}
            }},
            {"$sort": {"_id.date": 1, "_id.title": 1}}
        ]
        
        daily_sentiment_report = self.collection_name.aggregate(pipeline)
    
        for doc in daily_sentiment_report:
            date = doc['_id']['date']
            title = doc['_id']['title']
            total_sentences = doc['count']
            positive_count = doc['positive_count']
            neutral_count = doc['neutral_count']
            negative_count = doc['negative_count']
            
            positive_percentage = (positive_count / total_sentences) * 100 if total_sentences > 0 else 0
            neutral_percentage = (neutral_count / total_sentences) * 100 if total_sentences > 0 else 0
            negative_percentage = (negative_count / total_sentences) * 100 if total_sentences > 0 else 0
            
            print(f"Date: {date}, Title: {title}")
            print(f"Sentences: {total_sentences}, Average Sentiment: {doc['average_sentiment']}")
            print(f"Positive: {positive_count} ({positive_percentage:.2f}%), Neutral: {neutral_count} ({neutral_percentage:.2f}%), Negative: {negative_count} ({negative_percentage:.2f}%)")
            print("=" * 50)

    def generate_source_summary(self):
        """Generates a summary report for each source with sentiment counts, percentages, and top categories."""
        pipeline = [
            {
                "$group": {
                    "_id": "$source",
                    "total_sentences": {"$sum": 1},
                    "distinct_titles": {"$addToSet": "$cleaned_title"},
                    "all_categories": {"$push": "$category"},
                    
                    "avg_sentiment": {"$avg": "$Sentiment_Result"},
                    "positive_count": {
                        "$sum": {
                            "$cond": [
                                {"$eq": ["$sentiment_label", "positive"]}, 1, 0
                            ]
                        }
                    },
                    "neutral_count": {
                        "$sum": {
                            "$cond": [
                                {"$eq": ["$sentiment_label", "neutral"]}, 1, 0
                            ]
                        }
                    },
                    "negative_count": {
                        "$sum": {
                            "$cond": [
                                {"$eq": ["$sentiment_label", "negative"]}, 1, 0
                            ]
                        }
                    }
                }
            },
            { "$sort": {"avg_sentiment": -1} }
        ]
        
        source_summary = list(self.collection_name.aggregate(pipeline))
        
        print("Source Summary Report")
        print("="*80)
        for doc in source_summary:
            source = doc["_id"]
            total_sentences = doc["total_sentences"]
            total_articles  = len(doc["distinct_titles"])
            avg_sent        = doc["avg_sentiment"]
            pos_cnt         = doc["positive_count"]
            neu_cnt         = doc["neutral_count"]
            neg_cnt         = doc["negative_count"]
            
            pos_pct = pos_cnt / total_sentences * 100 if total_sentences else 0
            neu_pct = neu_cnt / total_sentences * 100 if total_sentences else 0
            neg_pct = neg_cnt / total_sentences * 100 if total_sentences else 0
            
            cat_counts = Counter(doc["all_categories"])
            top_cat, top_cat_cnt = cat_counts.most_common(1)[0]
            
            print(f"Source            : {source}")
            print(f"  Total Articles       : {total_articles}")
            print(f"  Total Sentences      : {total_sentences}")
            print(f"  Avg Sentiment        : {avg_sent:.2f}")
            print(f"  Positive Count       : {pos_cnt} , Positive Percentage : {pos_pct:.2f}%")
            print(f"  Neutral Count        : {neu_cnt} , Neutral Percentage  : {neu_pct:.2f}%")
            print(f"  Negative Count       : {neg_cnt} , Negative Percentage : {neg_pct:.2f}%")
            print(f"  Top Category         : {top_cat} ({top_cat_cnt}) sentences occurrences")
            print("-"*80)

    def get_total_sentences(self):
        """Calculates the total number of sentences across all articles in the collection."""
        pipeline = [
            {
                "$group": {
                    "_id": None,
                    "total_sentences": {"$sum": 1}
                }
            }
        ]
        
        total_sentences = self.collection_name.aggregate(pipeline)
        
        for record in total_sentences:
            print(f"Total Sentences in the Collection: {record['total_sentences']}")

    def get_unique_titles_count(self):
        """Calculates the total number of unique article titles in the collection."""
        pipeline = [
            {
                "$group": {
                    "_id": "$cleaned_title"
                }
            },
            {
                "$count": "unique_titles"
            }
        ]
        
        unique_titles_count = self.collection_name.aggregate(pipeline)
        
        for record in unique_titles_count:
            print(f"Total Unique Titles in the Collection: {record['unique_titles']}")

    def monthly_sentiment_distribution_by_date_range(self, start_date, end_date):
        """Generates monthly distribution of articles with counts per sentiment label for the given date range."""
        
        def convert_to_datetime(date_str):
            return datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")
    
        start_datetime = convert_to_datetime(start_date)
        end_datetime = convert_to_datetime(end_date)
    
        pipeline = [
            {"$addFields": {
                "publishedAt": {"$dateFromString": {"dateString": "$publishedAt", "format": "%Y-%m-%dT%H:%M:%SZ"}}}},
            {"$match": {"publishedAt": {"$gte": start_datetime, "$lte": end_datetime}}},
            {"$group": {
                "_id": {"year": {"$year": "$publishedAt"}, "month": {"$month": "$publishedAt"}, "sentiment": "$sentiment_label"},
                "count": {"$sum": 1}}},
            {"$sort": {"_id.year": 1, "_id.month": 1, "_id.sentiment": 1}}
        ]
    
        monthly_sentiment_distribution = list(self.collection_name.aggregate(pipeline))
    
        if monthly_sentiment_distribution:
            print(f"Monthly Sentiment Distribution from {start_date} to {end_date}:")
            for doc in monthly_sentiment_distribution:
                year_month = f"{doc['_id']['year']}-{doc['_id']['month']:02d}"
                print(f"\nYear-Month: {year_month}, Sentiment: {doc['_id']['sentiment']}, Count: {doc['count']}")
        else:
            print("No data found for the given date range.")
        
        return monthly_sentiment_distribution