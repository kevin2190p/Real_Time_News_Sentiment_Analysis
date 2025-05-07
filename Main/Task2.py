import logging
from typing import List, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.ml import PipelineModel

from Bulid_Modeling_Task2 import SentimentPipeline_Task2
from Data_Enrichment_Task2 import DataEnricher_Task2
from Data_Validation_Task2 import DataValidator_Task2


class NewsSentimentPipeline:
    """
    Encapsulates the full ETL and modeling workflow for news sentiment analysis:
      1. Load and validate raw articles
      2. Enrich validated data with NLP features
      3. Train or load a Spark ML sentiment model
      4. Save and serve predictions
    """

    def __init__(
        self,
        spark: SparkSession,
        input_path: str,
        enriched_output_path: str,
        pipeline_model_path: str,
        pipeline_pickle_path: Optional[str] = None,
        final_columns: Optional[List[str]] = None,
    ):
        # Configuration
        self.spark = spark
        self.input_path = input_path
        self.enriched_output_path = enriched_output_path
        self.pipeline_model_path = pipeline_model_path
        self.pipeline_pickle_path = pipeline_pickle_path
        self.final_columns = final_columns or []

        # Configure logging
        logging.basicConfig(
            format="%(asctime)s %(levelname)s %(message)s",
            level=logging.INFO,
        )
        self.logger = logging.getLogger(self.__class__.__name__)

    def run_validation(self) -> DataFrame:
        """
        Load raw data and run all validations. Returns cleaned DataFrame.
        """
        self.logger.info("Starting data validation")
        validator = DataValidator_Task2(self.spark)
        df = (
            validator
            .load_data(self.input_path)
            .run_all_validations()
            .get_clean_dataframe(
                drop_duplicates_url=True,
                filter_short_content=True
            )
        )
        if df is None or df.rdd.isEmpty():
            msg = "No valid data after validation"
            self.logger.error(msg)
            raise ValueError(msg)

        count = df.count()
        self.logger.info(f"Validation complete: {count} records remain")
        return df

    def run_enrichment(self, df: DataFrame) -> DataFrame:
        """
        Enrich the validated DataFrame with NLP-derived features.
        """
        self.logger.info("Starting data enrichment")
        enricher = DataEnricher_Task2(self.spark)
        enriched = (
            enricher
            .set_dataframe(df)
            .run_all_enrichments(final_columns_order=self.final_columns)
        )
        if enriched is None:
            msg = "Enrichment failed"
            self.logger.error(msg)
            raise RuntimeError(msg)

        self.logger.info("Enrichment complete; writing to output")
        enriched.write.mode("overwrite").parquet(self.enriched_output_path)
        return enriched

    def train_model(self, enriched_df: DataFrame) -> None:
        self.logger.info("Starting model training")
        pipeline = SentimentPipeline_Task2(
            spark_df=enriched_df,
            column_name="processed_sentence",
            pipeline_path=self.pipeline_model_path,
        )
        results = pipeline.run()

        self.logger.info(f"[DEBUG] pipeline.run() returned: {results!r}")
        if results is None:
            raise RuntimeError("SentimentPipeline.run() returned None!")
        self.logger.info(
            f"→ Training Accuracy: {results['train_accuracy']:.4f}, "
            f"Test Accuracy: {results['test_accuracy']:.4f}"
        )
        self.logger.info("Model training and persistence complete")






    def load_model(self) -> PipelineModel:
        """
        Load a saved Spark ML pipeline model for prediction.
        """
        self.logger.info(f"Loading pipeline model from {self.pipeline_model_path}")
        return PipelineModel.load(self.pipeline_model_path)

    def predict(self, texts: List[str]) -> List[float]:
        """
        Generate sentiment predictions for a list of text inputs.
        """
        # Prepare DataFrame for prediction
        df = self.spark.createDataFrame([(t,) for t in texts], ["processed_sentence"])
        model = self.load_model()
        predictions = model.transform(df)
        preds = [row.prediction for row in predictions.select("prediction").collect()]
        self.logger.info(f"Predictions: {preds}")
        return preds

    def run_full(self) -> None:
        """
        Orchestrates the full pipeline: validation, enrichment, training.
        """
        try:
            valid_df = self.run_validation()
            enriched_df = self.run_enrichment(valid_df)
            self.train_model(enriched_df)
            self.logger.info("Full pipeline execution finished successfully")
        except Exception as exc:
            self.logger.exception(f"Pipeline failed: {exc}")
        # Note: do not stop the Spark session here if further operations are needed


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("PetronasNewsSentiment") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    # Define paths and schema
    input_path = "file:///home/hduser/articles.parquet"
    enriched_output_path = "file:///home/student/de-prj/petronas_news_enriched_classes.parquet"
    pipeline_model_path = "file:///home/student/de-prj/spark_pipeline_model"
    final_columns = [
        "url", "title", "source", "source_domain", "publication_date", "category",
        "word_count", "people_mentioned", "organizations_mentioned", "locations_mentioned",
        "project_names", "financial_figures", "dates_mentioned", 
        "topic_label", "processed_sentence"
    ]

    pipeline = NewsSentimentPipeline(
        spark,
        input_path,
        enriched_output_path,
        pipeline_model_path,
        final_columns
    )
    # Run full pipeline
    pipeline.run_full()

    # Sample predictions
    sample_texts = [
        "I absolutely loved the new product, it's fantastic!",
        "The service was terrible and I am never coming back.",
        "The experience was okay, not too bad.",
        "Absolutely wonderful! I had a great time.",
        "It was a happy."
    ]

    preds = pipeline.predict(sample_texts)
    print(f"\nSample predictions: {preds}")

    # Stop Spark session after all operations
    spark.stop()

