from pymongo import MongoClient
from datetime import datetime
import pandas as pd
from collections import Counter

# Replace with your connection string and password
uri = "mongodb+srv://HongLik:SHL24112004shl@cluster0.dqhni.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

class Query_and_Report:
    def __init__(self, uri, db, collection):
        """
        Initialize the MongoDB client and connection to the specific database and collection.
        
        Parameters:
        db_uri (str): MongoDB URI to connect to the database
        db_name (str): The name of the database to connect to
        collection_name (str): The name of the collection to query from
        """
        # Initialize MongoDB connection
        self.client = MongoClient(uri)
        self.db = self.client["sentiment_analysis_database"]
        self.collection = self.db["sentiment_dataa"]

    def get_positive_sentiment_sentences_by_articles(self):
        """
        This function finds articles with positive sentiment, calculates average sentiment scores,
        and displays the sentiment result distribution along with the article sentences.
        
        Returns:
        positive_sentiment_sentences_by_articles (list): Aggregated list of articles with positive sentiment sentences
        """
        # MongoDB Aggregation Pipeline
        pipeline = [
            # Step 1: Match documents where sentiment_label is positive and Sentiment_Result > 0
            {"$match": {
                "sentiment_label": "positive", 
                "Sentiment_Result": {"$gt": 0}  # You can adjust the condition as per requirement
            }},
            
            # Step 2: Group by title, collect sentences, calculate positive sentence count, and calculate average sentiment
            {"$group": {
                "_id": "$cleaned_title",  # Group by article title
                "sentences": {"$push": "$sentence"},  # Collect all sentences for each article
                "positive_count": {"$sum": 1},  # Count of positive sentences
                "average_sentiment": {"$avg": "$Sentiment_Result"},  # Calculate the average sentiment score
                "sentiment_results": {"$push": "$Sentiment_Result"}  # Collect all sentiment results for each sentence
            }},
            
            # Step 3: Sort by the count of positive sentences, descending
            {"$sort": {"positive_count": -1}}  # Sort by the number of positive sentences
        ]
        
        # Step 4: Execute the aggregation pipeline
        positive_sentiment_sentences_by_articles = self.collection.aggregate(pipeline)
    
        # Display results with complexity
        print("Positive Sentiment Articles (sorted by positive sentence count):")
        for doc in positive_sentiment_sentences_by_articles:
            # Display title, total sentences, sentiment label, and sentiment result (average)
            print(f"\nTitle: {doc['_id']}")
            print(f"Total Sentences: {len(doc['sentences'])}")
            print(f"Sentiment Label: All Positive")
            
            # Display total counts for sentiment results 1 and 2
            sentiment_result_1_count = sum(1 for res in doc['sentiment_results'] if res == 1)
            sentiment_result_2_count = sum(1 for res in doc['sentiment_results'] if res == 2)
            print(f"Total Sentiment Result 1: {sentiment_result_1_count}")
            print(f"Total Sentiment Result 2: {sentiment_result_2_count}")
            
            # Print the table of sentences with sentiment result for each sentence
            print(f"\n{'#':<5} {'Sentence':<100} {'Sentiment Result':<20}")
            print("="*120)  # Separator for table rows
            
            # Enumerate through the sentences and print each with its sentiment result
            for idx, (sentence, sentiment_result) in enumerate(zip(doc['sentences'], doc['sentiment_results']), 1):
                print(f"{idx:<5} {sentence:<100} {sentiment_result:<20}")
            
            # Print the average sentiment result
            print(f"\nAverage Sentiment Result: {doc['average_sentiment']}")
            
            # Print a separator after each document for better readability
            print("=" * 120)
        return positive_sentiment_sentences_by_articles

    def get_negative_sentiment_sentences_by_articles(self):
        """
        This function finds articles with negative sentiment, calculates average sentiment scores,
        and displays the sentiment result distribution along with the article sentences.
        
        Returns:
        negative_sentiment_sentences_by_articles (list): Aggregated list of articles with negative sentiment sentences
        """
        # MongoDB Aggregation Pipeline
        pipeline = [
            # Step 1: Match documents where sentiment_label is negative and Sentiment_Result < 0
            {"$match": {
                "sentiment_label": "negative", 
                "Sentiment_Result": {"$lt": 0}  # Adjust condition to negative sentiment
            }},
            
            # Step 2: Group by title, collect sentences, calculate negative sentence count, and calculate average sentiment
            {"$group": {
                "_id": "$cleaned_title",  # Group by article title
                "sentences": {"$push": "$sentence"},  # Collect all sentences for each article
                "negative_count": {"$sum": 1},  # Count of negative sentences
                "average_sentiment": {"$avg": "$Sentiment_Result"},  # Calculate the average sentiment score
                "sentiment_results": {"$push": "$Sentiment_Result"}  # Collect all sentiment results for each sentence
            }},
            
            # Step 3: Sort by the count of negative sentences, descending
            {"$sort": {"negative_count": -1}}  # Sort by the number of negative sentences
        ]
        
        # Step 4: Execute the aggregation pipeline
        negative_sentiment_sentences_by_articles = self.collection.aggregate(pipeline)
    
        # Display results with complexity
        print("Negative Sentiment Articles (sorted by negative sentence count):")
        for doc in negative_sentiment_sentences_by_articles:
            # Display title, total sentences, sentiment label, and sentiment result (average)
            print(f"\nTitle: {doc['_id']}")
            print(f"Total Sentences: {len(doc['sentences'])}")
            print(f"Sentiment Label: All Negative")
            
            # Display total counts for sentiment results -1 and -2
            sentiment_result_neg1_count = sum(1 for res in doc['sentiment_results'] if res == -1)
            sentiment_result_neg2_count = sum(1 for res in doc['sentiment_results'] if res == -2)
            print(f"Total Sentiment Result -1: {sentiment_result_neg1_count}")
            print(f"Total Sentiment Result -2: {sentiment_result_neg2_count}")
            
            # Print the table of sentences with sentiment result for each sentence
            print(f"\n{'#':<5} {'Sentence':<100} {'Sentiment Result':<20}")
            print("="*120)  # Separator for table rows
            
            # Enumerate through the sentences and print each with its sentiment result
            for idx, (sentence, sentiment_result) in enumerate(zip(doc['sentences'], doc['sentiment_results']), 1):
                print(f"{idx:<5} {sentence:<100} {sentiment_result:<20}")
            
            # Print the average sentiment result
            print(f"\nAverage Sentiment Result: {doc['average_sentiment']}")
            
            # Print a separator after each document for better readability
            print("=" * 120)
            
        return negative_sentiment_sentences_by_articles

    
    ### Q3. Find articles with neutral or positive sentiment label and specific locations like 'Kuala Lumpur' or 'Malaysia
    def find_neutral_or_positive_sentiment_articles_with_location(self, location_1, location_2, n):
        """
        Find articles with neutral or positive sentiment label and specific locations like 'Kuala Lumpur' or 'Malaysia'.
        """
        pipeline = [
            # Step 1: Match articles where sentiment_label is either "neutral" or "positive" and location is either "Kuala Lumpur" or "Malaysia"
            {"$match": {
                "sentiment_label": {"$in": ["neutral", "positive"]},  # Filter articles with neutral or positive sentiment
                "locations_mentioned": {"$in": [location_1, location_2]}  # Filter by the specified locations (either "Kuala Lumpur" or "Malaysia")
            }},
            
            # Step 2: Sort articles by published date (most recent first)
            {"$sort": {"publishedAt": -1}},  # Sort by publishedAt in descending order
            
            # Step 3: Limit to the top n most recent articles
            {"$limit": n},
            
            # Step 4: Project the necessary fields (optional, you can adjust based on your needs)
            {"$project": {
                "_id": 0,  # Do not include the MongoDB _id field
                "title": "$cleaned_title",
                "cleaned_description": 1,
                "publishedAt": 1,
                "sentiment_label": 1,
                "locations_mentioned": 1,
                "sentence": 1
            }}
        ]
        
        # Execute the aggregation pipeline
        neutral_or_positive_sentiment_articles_with_location = list(self.collection.aggregate(pipeline))
        
        # Display results
        print(f"Found {len(neutral_or_positive_sentiment_articles_with_location)} neutral or positive sentiment articles with location '{location_1}' or '{location_2}'")
        for idx, article in enumerate(neutral_or_positive_sentiment_articles_with_location, 1):
            print("=" * 50)  # Separator for readability
            print(f"\nArticle {idx}:")
            print(f"Title: {article['title']}")
            print(f"Description: {article['cleaned_description']}")        
            print(f"Published At: {article['publishedAt']}")
            print(f"Sentiment Label: {article['sentiment_label']}")
            print(f"Locations Mentioned: {', '.join(article['locations_mentioned'])}")
            print(f"Sentence: {article['sentence']}\n")
    
        return neutral_or_positive_sentiment_articles_with_location

    
    ### Q4. Find the most common categories associated with a given sentiment label
    def get_most_common_categories(self, sentiment_label, top_n):
        """
        This function fetches the most common categories associated with a specified sentiment.
        
        Parameters:
        sentiment_label (str): The sentiment label to filter by ('positive' or 'negative').
        top_n (int): The number of top categories to retrieve.
        
        Returns:
        most_common_categories (list): A list of tuples with categories and their counts.
        """
        # Aggregate query to get category count based on sentiment label
        pipeline = [
            {"$match": {"sentiment_label": sentiment_label}},
            {"$group": {
                "_id": "$category",
                "count": {"$sum": 1}
            }},
            {"$sort": {"count": -1}},  # Sort by category count in descending order
            {"$limit": top_n}  # Limit to top_n categories
        ]
        
        most_common_categories = list(self.collection.aggregate(pipeline))
        
        # Display the most common categories in a neat tabular format
        print(f"\nTop {top_n} categories associated with {sentiment_label} sentiment:")
        
        # Convert the result into a pandas DataFrame for better readability
        df = pd.DataFrame(most_common_categories)
        df.columns = ['Category', 'Count']  # Rename columns
        
        # Display the table
        print(df.to_string(index=False))
        
        return most_common_categories

    
    ### Q5. Latest Sentiment Trend: retrieve articles sorted by the published date (from current to old)
    def get_articles_by_published_date(self, start_date, end_date, limit):
        """
        Retrieve articles sorted by the published date (from current to old):
        - Grouping by title and description
        - Showing the average sentiment result based on sentences
        - Showing the total sentence count
        - Including sentiment label, source, category, and published date.
        
        Arguments:
        start_date (str): Optional. The start date to filter articles.
        end_date (str): Optional. The end date to filter articles.
        limit (int): The number of articles to fetch. Default is 100.
        """
        # Build the match conditions based on the provided dates (if any)
        match_conditions = {}
        if start_date and end_date:
            match_conditions = {
                "publishedAt": {"$gte": start_date, "$lte": end_date}
            }
    
        # MongoDB aggregation pipeline
        pipeline = [
            # Step 1: Match articles by date range and other criteria if necessary
            {"$match": match_conditions},  # Only articles within the date range
            
            # Step 2: Add additional fields if needed (e.g., formatted date, sentiment group, etc.)
            {"$project": {
                "publishedAt": 1,
                "cleaned_title": 1,
                "cleaned_description": 1,
                "sentiment_label": 1,
                "source": 1,
                "category": 1,
                "sentence": 1,
                "url": 1,
                "Sentiment_Result": 1  # Add the Sentiment_Result for each sentence
            }},
            
            # Step 3: Group by title and description
            {"$group": {
                "_id": {"title": "$cleaned_title", "description": "$cleaned_description"},  # Group by title and description
                "sentiment_labels": {"$addToSet": "$sentiment_label"},  # Collect all sentiment labels for the article
                "sentences": {"$push": "$sentence"},  # Collect all sentences for each article
                "sentiment_results": {"$push": "$Sentiment_Result"},  # Collect all sentiment results for each sentence
                "sources": {"$addToSet": "$source"},  # Collect all sources for the title
                "categories": {"$addToSet": "$category"},  # Collect all categories for the title
                "publishedAt": {"$first": "$publishedAt"},  # Get the first published date
                "url": {"$first": "$url"}  # Get the first URL
            }},
            
            # Step 4: Add a field for the total sentence count and calculate the average sentiment result
            {"$addFields": {
                "sentence_count": {"$size": "$sentences"},  # Calculate total sentence count
                "average_sentiment_result": {
                    "$avg": "$sentiment_results"  # Calculate average sentiment result based on Sentiment_Result
                }
            }},
            
            # Step 5: Sort by the most recent published date first (from current to old)
            {"$sort": {"publishedAt": -1}},  # Sort by published date in descending order
            
            # Step 6: Limit the number of articles
            {"$limit": limit}
        ]
        
        # Execute the aggregation pipeline
        articles_by_published_date = list(self.collection.aggregate(pipeline))
        
        # Display the results
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

    
    ### Q6. Mixed Sentiment Articles with Percentiles
    def find_mixed_sentiment_articles_with_percentiles(self):
        # MongoDB aggregation pipeline to find articles with mixed sentiment and calculate percentiles manually
        pipeline = [
            # Step 1: Group by title and collect the sentiments and count
            {"$group": {
                "_id": "$cleaned_title",  # Group by article title
                "sentiments": {"$addToSet": "$sentiment_label"},  # Collect all unique sentiments for the article
                "count": {"$sum": 1},  # Count how many sentences belong to each title
                "positive_count": {"$sum": {"$cond": [{"$eq": ["$sentiment_label", "positive"]}, 1, 0]}},  # Count of positive sentiments
                "neutral_count": {"$sum": {"$cond": [{"$eq": ["$sentiment_label", "neutral"]}, 1, 0]}},  # Count of neutral sentiments
                "negative_count": {"$sum": {"$cond": [{"$eq": ["$sentiment_label", "negative"]}, 1, 0]}},  # Count of negative sentiments
                "sources": {"$addToSet": "$source"},  # Collect all sources for the title
                "urls": {"$addToSet": "$url"},  # Collect all urls for the title
                "cleaned_description": {"$first": "$cleaned_description"},  # Get the cleaned description of the article
                "publishedAt": {"$first": "$publishedAt"}  # Get the published date of the article
            }},
            # Step 2: Match articles where there are multiple sentiments
            {"$match": {
                "$expr": {"$gt": [{"$size": "$sentiments"}, 1]}  # Ensure there are multiple sentiments (mixed sentiment)
            }},
            # Step 3: Sort by the number of sentences in the article
            {"$sort": {"count": -1}}  # Sort by the count of sentences in descending order
        ]
        
        # Execute the aggregation pipeline
        mixed_sentiment_articles = list(self.collection.aggregate(pipeline))
        
        # Display results
        print(f"Found {len(mixed_sentiment_articles)} articles with mixed sentiment and percentiles calculated")
        for article in mixed_sentiment_articles:
            # Manually calculate percentiles for sentiment labels
            positive_count = article['positive_count']
            neutral_count = article['neutral_count']
            negative_count = article['negative_count']
            
            total_count = positive_count + neutral_count + negative_count
            
            # Calculate percentage for each sentiment label
            if total_count > 0:
                positive_percentage = (positive_count / total_count) * 100
                neutral_percentage = (neutral_count / total_count) * 100
                negative_percentage = (negative_count / total_count) * 100
            else:
                positive_percentage = neutral_percentage = negative_percentage = 0
            
            # Manually calculate percentiles for positive, neutral, and negative sentiments
            # Positive Sentiment Percentile
            positive_percentile = round(positive_percentage, 2)
            # Neutral Sentiment Percentile
            neutral_percentile = round(neutral_percentage, 2)
            # Negative Sentiment Percentile
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
            print("=" * 50)  # Separator after each record
    
        return mixed_sentiment_articles


    ### Q7.1 Articles with Strong Positive Sentiment
    def find_top_positive_sentiment_articles(self):
        """
        Find articles with the most positive sentiment and highest number of sentences.
        """
        pipeline = [
            {"$match": {"sentiment_label": "positive"}},  # Match only positive sentiment
            {"$group": {
                "_id": "$cleaned_title",  # Group by article title
                "positive_count": {"$sum": 1},  # Count of positive sentences
                "sentences": {"$push": "$sentence"}  # Collect all sentences
            }},
            {"$sort": {"positive_count": -1}},  # Sort by the number of positive sentences
            {"$limit": 10}  # Limit to top 10 articles
        ]
            
        # Execute the aggregation pipeline
        top_positive_sentiment_articles = list(self.collection.aggregate(pipeline))
            
        # Display results
        print(f"Top Positive Sentiment Articles:")
        for article in top_positive_sentiment_articles:
            print("=" * 50) 
            print(f"\nTitle: {article['_id']}\n")
            print(f"Positive Sentiment Count: {article['positive_count']}\n")
            for idx, sentence in enumerate(article['sentences'], 1):
                print(f"Sentence {idx}: {sentence}")
        
        return top_positive_sentiment_articles

        
    ### Q7.2 Articles with Strong Negative Sentiment
    def find_top_negative_sentiment_articles(self):
        """
        Find articles with the most negative sentiment (Sentiment_Result <= -2) and highest number of sentences.
        """
        pipeline = [
            {"$match": {"Sentiment_Result": {"$lte": -2}}},  # Match articles with strong negative sentiment
            {"$group": {
                "_id": "$cleaned_title",  # Group by article title
                "negative_count": {"$sum": 1},  # Count of negative sentences
                "sentences": {"$push": "$sentence"}  # Collect all sentences
            }},
            {"$sort": {"negative_count": -1}},  # Sort by the number of negative sentences
            {"$limit": 10}  # Limit to top 10 articles
        ]
            
        # Execute the aggregation pipeline
        top_negative_sentiment_articles = list(self.collection.aggregate(pipeline))
        
        # Display results
        print(f"Top Negative Sentiment Articles:")
        for article in top_negative_sentiment_articles:
            print("=" * 50) 
            print(f"\nTitle: {article['_id']}\n")
            print(f"Negative Sentiment Count: {article['negative_count']}\n")
            for idx, sentence in enumerate(article['sentences'], 1):
                print(f"Sentence {idx}: {sentence}")
        
        return top_negative_sentiment_articles

    
    ### Q8. Keyword Alerts in Title or Description
    def search_articles_with_sentences_by_keyword(self, keyword):
        """
        Search for articles where the title or description contains the specific keyword.
        Group by article title and show the sentences with sentiment labels for each article.
        
        Parameters:
        keyword (str): The keyword to search for in article titles or descriptions.
        
        Returns:
        results (list): A list of articles that match the keyword search.
        """
        pipeline = [
            # Step 1: Match articles containing the keyword in the title or description
            {"$match": {
                "$or": [
                    {"cleaned_title": {"$regex": keyword, "$options": "i"}},  # Match keyword in title, case-insensitive
                    {"cleaned_description": {"$regex": keyword, "$options": "i"}}  # Match keyword in description, case-insensitive
                ]
            }},
            
            # Step 2: Group by title and aggregate sentences, sentiment labels, and counts
            {"$group": {
                "_id": "$cleaned_title",  # Group by article title
                "sentences": {"$push": "$sentence"},  # Collect all sentences
                "sentiment_labels": {"$addToSet": "$sentiment_label"},  # Collect all unique sentiment labels
                "positive_count": {"$sum": {"$cond": [{"$eq": ["$sentiment_label", "positive"]}, 1, 0]}},  # Count of positive sentiments
                "neutral_count": {"$sum": {"$cond": [{"$eq": ["$sentiment_label", "neutral"]}, 1, 0]}},  # Count of neutral sentiments
                "negative_count": {"$sum": {"$cond": [{"$eq": ["$sentiment_label", "negative"]}, 1, 0]}},  # Count of negative sentiments
                "sources": {"$addToSet": "$source"},  # Collect all sources for the title
                "urls": {"$addToSet": "$url"}  # Collect all URLs for the title
            }},
            
            # Step 3: Sort by the count of sentences (can also sort by title or sentiment count)
            {"$sort": {"positive_count": -1}}  # Sort by the number of positive sentences first
        ]
        
        # Execute the aggregation pipeline
        articles_with_sentences_by_keyword = list(self.collection.aggregate(pipeline))
        
        # Display results
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
            
    
            print("=" * 50)  # Separator after each record

    
    ### Q9. Trending Topics (using content field)
    def extract_articles_by_keywords(self, keywords, start_date=None, end_date=None, limit=10):
        """
        Extract articles containing specific keywords in their content. Allows for filtering by date range.
        
        Parameters:
        keywords (list): A list of keywords to search for in the article sentences.
        start_date (str): Optional start date to filter articles by (ISO 8601 format).
        end_date (str): Optional end date to filter articles by (ISO 8601 format).
        limit (int): The maximum number of articles to retrieve (default is 10).
        
        Returns:
        articles_by_keywords (list): List of articles containing the specified keywords.
        """
        # Build the match conditions for the keywords and optional date range
        match_conditions = {
            "sentence": {"$regex": "|".join(keywords), "$options": "i"}  # Match any of the keywords in sentence (case-insensitive)
        }
        
        if start_date and end_date:
            match_conditions["publishedAt"] = {"$gte": start_date, "$lte": end_date}  # Filter by date range
        
        # MongoDB aggregation pipeline
        pipeline = [
            # Step 1: Match articles that contain the specified keywords in the sentence
            {"$match": match_conditions},
            
            # Step 2: Group by title and collect the sentences that match the keywords
            {"$group": {
                "_id": "$cleaned_title",  # Group by article title
                "sentences": {"$push": "$sentence"},  # Collect sentences matching the keywords
                "sentiment_label": {"$first": "$sentiment_label"},  # Take the sentiment label of the article
                "publishedAt": {"$first": "$publishedAt"},  # Take the first published date of the article
                "source": {"$first": "$source"},  # Collect the source of the article
                "url": {"$first": "$url"}  # Collect the URL of the article
            }},
            
            # Step 3: Sort by the published date to show the most recent articles first
            {"$sort": {"publishedAt": -1}},  # Sort by published date descending
            
            # Step 4: Limit the number of results
            {"$limit": limit}
        ]
        
        # Execute the aggregation pipeline
        articles_by_keywords = list(self.collection.aggregate(pipeline))
        
        # Display the results
        if articles_by_keywords:
            print(f"Articles containing the keywords {', '.join(keywords)}:")
            for doc in articles_by_keywords:
                print("=" * 50)
                print(f"\nTitle: {doc['_id']}")
                print(f"Published At: {doc['publishedAt']}")
                print(f"Sentiment Label: {doc['sentiment_label']}")
                print(f"Source: {doc['source']}")
                print(f"URL: {doc['url']}")
                
                # Show the sentences with the keywords
                print("\nSentences with the keywords:")
                for idx, sentence in enumerate(doc['sentences'], 1):
                    print(f"Sentence {idx}: {sentence}")
                
                print("=" * 50)  # Separator after each record
        else:
            print("No articles found for the given keywords.")
    
        return articles_by_keywords

    
    ### R1: Generate Daily Sentiment Report
    def get_daily_sentiment_report_with_percentages(self):
        """
        Generates a daily sentiment report for articles, calculates the average sentiment score,
        and provides the percentage distribution of each sentiment label (positive, neutral, negative).
        
        Returns:
        result (list): A list of daily sentiment reports.
        """
        # MongoDB aggregation pipeline to group articles by date and title, and calculate sentiment counts
        pipeline = [
            # Step 1: Group by date and title and aggregate the sentiment counts and average sentiment
            {"$group": {
                "_id": {
                    "date": {"$substr": ["$publishedAt", 0, 10]},  # Group by date (YYYY-MM-DD)
                    "title": "$cleaned_title"  # Group by article title
                },
                "count": {"$sum": 1},  # Count of sentences
                "average_sentiment": {"$avg": "$Sentiment_Result"},  # Average sentiment score
                "positive_count": {"$sum": {"$cond": [{"$eq": ["$sentiment_label", "positive"]}, 1, 0]}},
                "neutral_count": {"$sum": {"$cond": [{"$eq": ["$sentiment_label", "neutral"]}, 1, 0]}},
                "negative_count": {"$sum": {"$cond": [{"$eq": ["$sentiment_label", "negative"]}, 1, 0]}}
            }},
            
            # Step 2: Sort by date and title
            {"$sort": {"_id.date": 1, "_id.title": 1}}  # Sort by date and then by title
        ]
        
        # Execute the aggregation pipeline
        daily_sentiment_report = self.collection.aggregate(pipeline)
    
        # Print the daily sentiment report with percentage calculations
        for doc in daily_sentiment_report:
            date = doc['_id']['date']
            title = doc['_id']['title']
            total_sentences = doc['count']
            positive_count = doc['positive_count']
            neutral_count = doc['neutral_count']
            negative_count = doc['negative_count']
            
            # Calculate percentages for each sentiment label
            positive_percentage = (positive_count / total_sentences) * 100 if total_sentences > 0 else 0
            neutral_percentage = (neutral_count / total_sentences) * 100 if total_sentences > 0 else 0
            negative_percentage = (negative_count / total_sentences) * 100 if total_sentences > 0 else 0
            
            # Print the results
            print(f"Date: {date}, Title: {title}")
            print(f"Sentences: {total_sentences}, Average Sentiment: {doc['average_sentiment']}")
            print(f"Positive: {positive_count} ({positive_percentage:.2f}%), Neutral: {neutral_count} ({neutral_percentage:.2f}%), Negative: {negative_count} ({negative_percentage:.2f}%)")
            print("=" * 50)  # Separator between each record for better readability


    ### R2. Generates Source Summary Report
    def generate_source_summary(self):
        """
        Generates a summary report for each source with sentiment counts, percentages, and top categories.
        
        Returns:
        source_summary (list): A summary report for each source.
        """
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
        
        source_summary = list(self.collection.aggregate(pipeline))
        
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
            
            # percentages
            pos_pct = pos_cnt / total_sentences * 100 if total_sentences else 0
            neu_pct = neu_cnt / total_sentences * 100 if total_sentences else 0
            neg_pct = neg_cnt / total_sentences * 100 if total_sentences else 0
            
            # top category
            cat_counts = Counter(doc["all_categories"])
            top_cat, top_cat_cnt = cat_counts.most_common(1)[0]
            
            # print
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
        """
        Calculates the total number of sentences across all articles in the collection.
        
        Returns:
        result (list): A list with the total sentence count.
        """
        pipeline = [
            {
                # Grouping by nothing to count all sentences
                "$group": {
                    "_id": None,  # No grouping by title
                    "total_sentences": {"$sum": 1}  # Count the total number of sentences
                }
            }
        ]
        
        # Execute the aggregation pipeline
        total_sentences = self.collection.aggregate(pipeline)
        
        # Output the result
        for record in total_sentences:
            print(f"Total Sentences in the Collection: {record['total_sentences']}")


    def get_unique_titles_count(self):
        """
        Calculates the total number of unique article titles in the collection.
        
        Returns:
        result (list): A list with the unique title count.
        """
        pipeline = [
            {
                # Group by title to count unique titles
                "$group": {
                    "_id": "$cleaned_title"  # Group by the cleaned title field
                }
            },
            {
                # Count the number of unique titles
                "$count": "unique_titles"
            }
        ]
        
        # Execute the aggregation pipeline
        unique_titles_count = self.collection.aggregate(pipeline)
        
        # Output the result
        for record in unique_titles_count:
            print(f"Total Unique Titles in the Collection: {record['unique_titles']}")

    
    ### R3. Generates monthly distribution of articles, sentences, sentiment labels
    def monthly_sentiment_distribution_by_date_range(self, start_date, end_date):
        """
        Generates monthly distribution of articles with counts per sentiment label (positive, neutral, negative)
        for the given date range.
        
        Parameters:
        start_date (str): The start date for filtering articles in ISO 8601 format.
        end_date (str): The end date for filtering articles in ISO 8601 format.
        
        Returns:
        monthly_sentiment_distribution (list): A list of monthly sentiment distribution results.
        """
        
        def convert_to_datetime(date_str):
            return datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")

        start_datetime = convert_to_datetime(start_date)
        end_datetime = convert_to_datetime(end_date)

        pipeline = [
            # Step 1: Convert 'publishedAt' string to date and add year and month based on the 'publishedAt' field
            {"$addFields": {
                "publishedAt": {"$dateFromString": {"dateString": "$publishedAt", "format": "%Y-%m-%dT%H:%M:%SZ"}}}},
            # Step 2: Match articles by date range
            {"$match": {"publishedAt": {"$gte": start_datetime, "$lte": end_datetime}}},
            # Step 3: Group by year, month, and sentiment label
            {"$group": {
                "_id": {"year": {"$year": "$publishedAt"}, "month": {"$month": "$publishedAt"}, "sentiment": "$sentiment_label"},
                "count": {"$sum": 1}}},
            # Step 4: Sort by year and month
            {"$sort": {"_id.year": 1, "_id.month": 1, "_id.sentiment": 1}}
        ]

        # Execute the aggregation pipeline
        monthly_sentiment_distribution = list(self.collection.aggregate(pipeline))

        # Display the results
        if monthly_sentiment_distribution:
            print(f"Monthly Sentiment Distribution from {start_date} to {end_date}:")
            for doc in monthly_sentiment_distribution:
                year_month = f"{doc['_id']['year']}-{doc['_id']['month']:02d}"
                print(f"\nYear-Month: {year_month}, Sentiment: {doc['_id']['sentiment']}, Count: {doc['count']}")
        else:
            print("No data found for the given date range.")
    
    return monthly_sentiment_distribution
