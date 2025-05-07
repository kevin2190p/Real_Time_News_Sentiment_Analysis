################################################################################
# Sentiment Pipeline Class Using Spark ML with Train/Test Evaluation
################################################################################


import os
import numpy as np
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, StopWordsRemover, CountVectorizer

class SentimentPipeline_Task2:
    """
    Encapsulates the full pipeline process using Spark ML:
      - Collect sentences from a Spark DataFrame.
      - Generate numerical sentiment labels using a Hugging Face pipeline.
      - Split into train/test sets.
      - Build, train, and evaluate a Spark ML pipeline (TF-IDF + Logistic Regression).
      - Save the trained pipeline model.
    """
    def __init__(self, spark_df=None, column_name="processed_sentence",
                 pipeline_path="file:///home/student/de-prj/sentiment_pipeline_spark",
                 train_ratio=0.8, seed=42):
        self.spark_df = spark_df
        self.column_name = column_name
        self.pipeline_path = pipeline_path
        self.train_ratio = train_ratio
        self.seed = seed

        self.sentences = []
        self.labels = []
        self.pipeline_model = None

    def collect_sentences(self):
        if self.spark_df is None:
            raise ValueError("No Spark DataFrame provided. Please set spark_df before collecting sentences.")
        self.sentences = self.spark_df.select(self.column_name).rdd.flatMap(lambda x: x).collect()
        print(f"Collected {len(self.sentences)} sentences.")
        if not self.sentences:
            raise ValueError("No sentences collected.")
        return self.sentences

    def generate_sentiment_labels(self):
        from transformers import pipeline as hf_pipeline
        sentiment_pipe = hf_pipeline("text-classification", model="tabularisai/multilingual-sentiment-analysis")
        self.labels = []
        for sentence in self.sentences:
            try:
                result = sentiment_pipe(sentence)[0]
                mapping = {
                    'very negative': 1,
                    'negative': 2,
                    'neutral': 3,
                    'positive': 4,
                    'very positive': 5
                }
                self.labels.append(mapping.get(result['label'].lower(), 3))
            except Exception:
                self.labels.append(3)
        counts = dict(zip(*np.unique(self.labels, return_counts=True)))
        print("Label distribution:", counts)
        return self.labels

    def build_and_evaluate_pipeline(self):
        if not self.sentences or not self.labels:
            raise ValueError("Sentences or labels missing. Run collect_sentences and generate_sentiment_labels first.")

        spark = SparkSession.builder.getOrCreate()
        data = list(zip(self.sentences, self.labels))
        df = spark.createDataFrame(data, schema=[self.column_name, "label"]).cache()

        train_df, test_df = df.randomSplit([self.train_ratio, 1 - self.train_ratio], seed=self.seed)
        print(f"Train set: {train_df.count()} rows, Test set: {test_df.count()} rows.")

        tokenizer = Tokenizer(inputCol=self.column_name, outputCol="words")
        remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
        try:
            cv = CountVectorizer(inputCol="filtered_words", outputCol="tf", vocabSize=10000, minDF=1.0)
            idf = IDF(inputCol="tf", outputCol="features", minDocFreq=1)
            pipeline_stages = [tokenizer, remover, cv, idf]
        except Exception:
            print("CountVectorizer failed, falling back to HashingTF...")
            hashingTF = HashingTF(inputCol="filtered_words", outputCol="tf", numFeatures=100)
            idf = IDF(inputCol="tf", outputCol="features", minDocFreq=1)
            pipeline_stages = [tokenizer, remover, hashingTF, idf]

        lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=1000)
        complete_pipeline = Pipeline(stages=pipeline_stages + [lr])

        print("Fitting pipeline on training data...")
        self.pipeline_model = complete_pipeline.fit(train_df)

        evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
        train_acc = evaluator.evaluate(self.pipeline_model.transform(train_df))
        test_acc = evaluator.evaluate(self.pipeline_model.transform(test_df))
        print(f"Training Accuracy: {train_acc:.4f}")
        print(f"Test Accuracy: {test_acc:.4f}")

        return {"model": self.pipeline_model, "train_accuracy": train_acc, "test_accuracy": test_acc}

    def save_pipeline(self):
        if self.pipeline_model is None:
            raise ValueError("No trained pipeline to save. Please run build_and_evaluate_pipeline first.")
        self.pipeline_model.write().overwrite().save(self.pipeline_path)
        print(f"Pipeline saved to '{self.pipeline_path}'")

    def run(self):
        self.collect_sentences()
        self.generate_sentiment_labels()
        results = self.build_and_evaluate_pipeline()
        self.save_pipeline()
        return results
