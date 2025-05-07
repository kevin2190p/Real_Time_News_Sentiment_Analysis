# Core Spark Imports
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, sum, length, count, regexp_extract, udf, lit, split, array_contains,
    size, to_date, coalesce, format_string, lpad, explode, isnull
)
from pyspark.sql.types import (
    IntegerType, StringType, ArrayType, StructType, StructField, FloatType,
    DateType, TimestampType
)

# ML and NLP Imports
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer
from pyspark.ml.clustering import LDA
import spacy
from pyspark.sql import functions as F

# Standard Python Libraries
import re
from urllib.parse import urlparse
from datetime import datetime
from collections import OrderedDict
import os

class DataEnricher:
    """
    Enriches the news articles DataFrame with metadata, entities, topics, etc.
    """
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.df = None
        self.nlp = None # To hold the loaded spaCy model
        self._initialize_spacy() # Attempt to load spaCy on init
        print("DataEnricher initialized.")

    def set_dataframe(self, df: DataFrame) -> 'DataEnricher':
        """Sets the DataFrame to be enriched."""
        self.df = df
        if self.df:
            print(f"--- [Enricher] DataFrame set. Row count: {self.df.count()} ---")
        else:
            print("WARNING: [Enricher] Received an empty DataFrame.")
        return self

    def _initialize_spacy(self):
        """Loads the spaCy model."""
        if self.nlp is None:
            try:
                # Make sure you have the model downloaded:
                # python -m spacy download en_core_web_sm
                self.nlp = spacy.load("en_core_web_sm")
                print("spaCy model 'en_core_web_sm' loaded successfully.")
            except ImportError:
                print("WARNING: spaCy library not found. Cannot perform spaCy entity extraction. Install with: pip install spacy")
                self.nlp = None
            except OSError:
                 print("WARNING: spaCy model 'en_core_web_sm' not found. Download it: python -m spacy download en_core_web_sm")
                 self.nlp = None
            except Exception as e:
                print(f"WARNING: An unexpected error occurred loading spaCy: {e}")
                self.nlp = None

    def run_all_enrichments(self,
                            url_col="url", content_col="cleaned_content",
                            num_topics=5, lda_max_iter=15, lda_vocab_size=5000,
                            final_columns_order=None) -> DataFrame:
        """Runs all enrichment steps sequentially."""
        if self.df is None:
            print("ERROR: DataFrame not set in Enricher. Cannot run enrichments.")
            return None

        print("\n--- [Enricher] Running Data Enrichment Steps ---")
        self.enrich_metadata(url_col=url_col, content_col=content_col)
        self.enrich_entities(content_col=content_col)
        self.enrich_project_names(content_col=content_col)
        #self.enrich_topics(content_col=content_col, num_topics=num_topics, max_iter=lda_max_iter, vocab_size=lda_vocab_size)


        print("--- [Enricher] Enrichment steps completed ---")
        # Select final columns if order is specified
        if final_columns_order:
            self.df = self._select_final_columns(self.df, final_columns_order)
            
        return self.df

    def enrich_metadata(self, url_col="url", content_col="content",
                        potential_date_cols=["publish_date", "date", "published_at", "timestamp", "creation_date"],
                        target_date_col="publication_date"):
        """Adds metadata columns: source, domain, date, category, word count."""
        print("Enriching metadata...")
        if self.df is None: return

        # Add Source based on URL
        if url_col in self.df.columns:
            self.df = self.df.withColumn(
                "source",
                when(col(url_col).contains("reuters.com"), "Reuters")
                .when(col(url_col).contains("bloomberg.com"), "Bloomberg")
                .when(col(url_col).contains("nst.com"), "New Straits Times")
                .when(col(url_col).contains("thestar.com"), "The Star")
                .when(col(url_col).contains("theedgemarkets.com"), "The Edge Markets")
                .otherwise("Other")
            )
            # Add Domain
            domain_udf = self._get_domain_udf()
            self.df = self.df.withColumn("source_domain", domain_udf(col(url_col)))
        else:
            self.df = self.df.withColumn("source", lit("Unknown"))
            self.df = self.df.withColumn("source_domain", lit(None).cast(StringType()))


        # Add Category based on content
        if content_col in self.df.columns:
            self.df = self.df.withColumn(
                "category",
                when(col(content_col).rlike("(?i)financ|profit|revenue|earnings|dividend|investment|stock|market|shares"), "Financial")
                .when(col(content_col).rlike("(?i)sustainab|green|environment|carbon|climate|emission|esg|renewable"), "Sustainability")
                .when(col(content_col).rlike("(?i)gas|oil|drilling|exploration|discovery|field|reserves|lng|upstream|downstream|refinery"), "Exploration & Production")
                .when(col(content_col).rlike("(?i)partner|deal|agreement|contract|acquisition|merger|joint venture|mou"), "Business Deals")
                .when(col(content_col).rlike("(?i)technolog|digital|innovation|research|development|ai|platform"), "Technology & Innovation")
                .otherwise("General")
            )
        else:
             self.df = self.df.withColumn("category", lit("Unknown"))

        # Calculate Word Count
        if content_col in self.df.columns:
            self.df = self.df.withColumn(
                "word_count",
                when(col(content_col).isNotNull(), size(split(col(content_col), "\\s+"))).otherwise(0)
            )
        else:
            self.df = self.df.withColumn("word_count", lit(0).cast(IntegerType()))
        print("Metadata enrichment done (source, domain, date, category, word_count).")


    @staticmethod
    def _get_domain_udf():
        """Creates and returns the UDF for extracting domain from URL."""
        def get_domain(url):
            try:
                if url and isinstance(url, str) and url.startswith(('http://', 'https://')):
                    return urlparse(url).netloc
                return None
            except Exception: return None
        return udf(get_domain, StringType())

    

    def enrich_entities(self, content_col="content"):
        """Extracts named entities using spaCy."""
        print("Enriching entities using spaCy...")
        if self.df is None: return
        if content_col not in self.df.columns:
            print(f"Skipping entity extraction: Column '{content_col}' not found.")
            self._add_empty_entity_columns()
            return
        if self.nlp is None:
            print("Skipping entity extraction: spaCy model not loaded.")
            self._add_empty_entity_columns()
            return

        # Define UDF schema and function
        entity_schema = StructType([
            StructField("people", ArrayType(StringType()), True), StructField("organizations", ArrayType(StringType()), True),
            StructField("locations", ArrayType(StringType()), True), StructField("dates", ArrayType(StringType()), True),
            StructField("money", ArrayType(StringType()), True)
        ])
        extract_entities_udf = self._get_extract_entities_udf(self.nlp, entity_schema) # Pass loaded model

        # Apply UDF
        self.df = self.df.withColumn("extracted_entities", extract_entities_udf(col(content_col)))

        # Flatten struct into columns
        self.df = self.df.withColumn("people_mentioned", col("extracted_entities.people")) \
                           .withColumn("organizations_mentioned", col("extracted_entities.organizations")) \
                           .withColumn("locations_mentioned", col("extracted_entities.locations")) \
                           .withColumn("dates_mentioned", col("extracted_entities.dates")) \
                           .withColumn("financial_figures", col("extracted_entities.money")) \
                           .drop("extracted_entities")
        print("Entity extraction done.")


    @staticmethod
    def _get_extract_entities_udf(nlp_model, schema):
         """Creates the UDF for spaCy entity extraction."""
         if nlp_model is None: # Return a dummy UDF if spacy failed
             def dummy_extract(text):
                 return {"people": [], "organizations": [], "locations": [], "dates": [], "money": []}
             return udf(dummy_extract, schema)

         # --- Actual UDF function using the passed nlp_model ---
         def extract_entities(text):
             # This function now closes over the nlp_model variable passed to _get_extract_entities_udf
             entities = {"people": [], "organizations": [], "locations": [], "dates": [], "money": []}
             if not text or not isinstance(text, str): return entities
             try:
                 doc = nlp_model(text[:100000]) # Limit text size
                 for ent in doc.ents:
                     ent_text = ent.text.strip()
                     if len(ent_text) < 3 or ent_text.isspace(): continue
                     label = ent.label_
                     if label == "PERSON" and len(ent_text.split()) <= 4: entities["people"].append(ent_text)
                     elif label == "ORG" and "petronas" not in ent_text.lower(): entities["organizations"].append(ent_text)
                     elif label in ["GPE", "LOC"]: entities["locations"].append(ent_text)
                     elif label == "DATE": entities["dates"].append(ent_text)
                     elif label == "MONEY": entities["money"].append(ent_text)
                 for key in entities: entities[key] = list(OrderedDict.fromkeys(entities[key]))[:10]
             except Exception as e: pass # Log errors if needed, but don't fail the UDF
             return entities
         # --- End of actual UDF function ---

         return udf(extract_entities, schema)

    def _add_empty_entity_columns(self):
         """Adds empty array columns if entity extraction is skipped."""
         if self.df is None: return
         entity_cols = ["people_mentioned", "organizations_mentioned", "locations_mentioned", "dates_mentioned", "financial_figures"]
         for col_name in entity_cols:
             if col_name not in self.df.columns:
                 self.df = self.df.withColumn(col_name, lit(None).cast(ArrayType(StringType())))

    def enrich_project_names(self, content_col="content"):
        """Extracts project names using regular expressions."""
        print("Enriching project names using regex...")
        if self.df is None: return
        if content_col not in self.df.columns:
            print(f"Skipping project name extraction: Column '{content_col}' not found.")
            self.df = self.df.withColumn("project_names", lit(None).cast(ArrayType(StringType())))
            return

        project_pattern = r"(?i)(?:Project|Basin|Field|Platform|Terminal|Plant|Block)\s+([A-Z][-a-zA-Z0-9\s]*[a-zA-Z0-9])"
        extract_projects_udf = udf(
            lambda text: list(OrderedDict.fromkeys([match.strip() for match in re.findall(project_pattern, text)])) if text else [],
            ArrayType(StringType())
        )
        self.df = self.df.withColumn("project_names", extract_projects_udf(col(content_col)))
        print("Project name extraction done.")


    def enrich_topics(self, content_col="content", num_topics=5, max_iter=15, vocab_size=5000, min_df=5):
        """Performs LDA topic modeling."""
        print("Enriching topics using LDA...")
        if self.df is None: return
        if content_col not in self.df.columns:
            print(f"Skipping topic modeling: Column '{content_col}' not found.")
            self._add_empty_topic_columns()
            return

        # Prepare data for LDA
        df_for_lda = self.df.select("url", content_col).fillna({content_col: ''}) # Use URL as identifier
        tokenizer = Tokenizer(inputCol=content_col, outputCol="tokens")
        df_tokens = tokenizer.transform(df_for_lda)
        custom_stopwords = ["petronas", "said", "also", "year", "company", "group", "malaysia", "kuala", "lumpur", "ringgit", "rm", "mln", "bln", "pct", "news", "report", "update", "inc", "bhd"]
        remover = StopWordsRemover(inputCol="tokens", outputCol="filtered_tokens")
        remover.setStopWords(StopWordsRemover.loadDefaultStopWords("english") + custom_stopwords)
        df_no_stop = remover.transform(df_tokens)
        vectorizer = CountVectorizer(inputCol="filtered_tokens", outputCol="features", vocabSize=vocab_size, minDF=min_df)
        try:
            cv_model = vectorizer.fit(df_no_stop)
            df_features = cv_model.transform(df_no_stop)

            # Train LDA
            lda = LDA(k=num_topics, maxIter=max_iter, featuresCol="features", seed=42)
            lda_model = lda.fit(df_features)
            df_with_topics = lda_model.transform(df_features)

            # Process results
            dominant_topic_udf = udf(lambda dist: int(max(enumerate(dist), key=lambda x: x[1])[0]) if dist else None, IntegerType())
            df_with_topics = df_with_topics.withColumn("dominant_topic", dominant_topic_udf(col("topicDistribution")))

            # Create topic labels
            vocab = cv_model.vocabulary
            topicDescDF = lda_model.describeTopics(maxTermsPerTopic=5)
            generic_stop_words = set(StopWordsRemover.loadDefaultStopWords("english") + custom_stopwords)
            def indices_to_words(termIndices):
                keywords = [vocab[i] for i in termIndices if vocab[i].lower() not in generic_stop_words and len(vocab[i]) > 2]
                return ", ".join(keywords[:3]) if keywords else "Unknown Topic"
            indices_to_words_udf = udf(indices_to_words, StringType())
            topicDescDF = topicDescDF.withColumn("topic_keywords", indices_to_words_udf(col("termIndices")))
            topic_mapping = {row['topic']: row['topic_keywords'] for row in topicDescDF.select("topic", "topic_keywords").collect()}
            topic_mapping_bc = self.spark.sparkContext.broadcast(topic_mapping)
            map_topic_label_udf = udf(lambda idx: topic_mapping_bc.value.get(idx, f"Unknown Topic {idx}") if idx is not None else "N/A", StringType())
            df_with_topics = df_with_topics.withColumn("topic_label", map_topic_label_udf(col("dominant_topic")))

            # Join results back (selecting only necessary topic columns)
            topic_results = df_with_topics.select("url", "dominant_topic", "topic_label") # "topicDistribution" removed - too large
            self.df = self.df.join(topic_results, on="url", how="left")
            print("Topic modeling enrichment done.")

        except Exception as e:
            print(f"ERROR during topic modeling: {e}. Skipping topic enrichment.")
            self._add_empty_topic_columns()


    def _add_empty_topic_columns(self):
        """Adds empty topic columns if LDA fails or is skipped."""
        if self.df is None: return
        if "dominant_topic" not in self.df.columns:
            self.df = self.df.withColumn("dominant_topic", lit(None).cast(IntegerType()))
        if "topic_label" not in self.df.columns:
            self.df = self.df.withColumn("topic_label", lit("N/A").cast(StringType()))


   

    @staticmethod
    def _get_process_content_udf():
         """Creates the UDF for sentence processing."""
         def process_content_sentences(input_text):
             # ... (Sentence splitting, cleaning, filtering logic from previous example) ...
             if input_text is None or not isinstance(input_text, str): return []
             sentences = re.split(r'[.?!]\s+|\n+', input_text)
             sentences = [s.strip() for s in sentences if s and not s.isspace()]
             unique_sentences = list(OrderedDict.fromkeys(sentences))
             final_sentences = []
             for sentence in unique_sentences:
                 words = sentence.split()
                 words_no_digits = [word for word in words if not any(char.isdigit() for char in word)]
                 cleaned_sentence = ' '.join(words_no_digits)
                 if len(cleaned_sentence.split()) >= 3:
                     final_sentences.append(cleaned_sentence)
             return final_sentences
         return udf(process_content_sentences, ArrayType(StringType()))

    @staticmethod
    def _select_final_columns(df, columns_order):
        """Selects and orders columns based on the provided list."""
        print("Selecting and ordering final columns...")
        existing_columns = [c for c in columns_order if c in df.columns]
        missing_columns = [c for c in columns_order if c not in df.columns]
        if missing_columns:
            print(f"Warning: Requested final columns not found in DataFrame: {missing_columns}")
        print(f"Final selected columns: {existing_columns}")
        return df.select(existing_columns)


