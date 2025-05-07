from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession

class SentimentPredictor:
    """
    A class for loading and using a PySpark ML pipeline for sentiment prediction.
    """
    
    def __init__(self, spark, pipeline_path="sentiment_pipeline_spark"):
        """
        Initialize the SentimentPredictor with a SparkSession and pipeline path.
        
        Args:
            spark (SparkSession): The active Spark session
            pipeline_path (str): Path to the saved pipeline model
        """
        self.spark = spark
        self.pipeline_path = pipeline_path
        self.model = None
        self._load_pipeline()
    
    def _load_pipeline(self):
        """
        Load the pipeline model from the specified path.
        """
        try:
            self.model = PipelineModel.load(self.pipeline_path)
            print(f"Loaded pipeline successfully from {self.pipeline_path}.")
        except Exception as e:
            print(f"Error loading pipeline from '{self.pipeline_path}': {e}")
            self.model = None
    
    def predict(self, sample_texts):
        """
        Make sentiment predictions on the provided texts.
        
        Args:
            sample_texts (str or list): A single text string or a list of text strings
            
        Returns:
            The first prediction if successful, None if an error occurs
        """
        # Ensure sample_text is a list if provided as a single string
        if isinstance(sample_texts, str):
            sample_texts = [sample_texts]
        
        if self.model is None:
            print("Pipeline model not loaded. Cannot make predictions.")
            return None
        
        # Create a DataFrame from the input sample texts
        # The column name should match the expected input in your pipeline
        try:
            pred_df = self.spark.createDataFrame(
                [(text,) for text in sample_texts], 
                ["processed_sentence"]
            )
        except Exception as e:
            print(f"Error creating DataFrame from input texts: {e}")
            return None
        
        # Use the pipeline's transform method to get predictions
        try:
            predictions_df = self.model.transform(pred_df)
        except Exception as e:
            print(f"Error during transformation: {e}")
            return None
        
        # Extract the predictions from the DataFrame as a list
        try:
            prediction_list = predictions_df.select("prediction").rdd.map(
                lambda row: row.prediction
            ).collect()
            print("Predicted sentiment:", prediction_list)
            return prediction_list[0] 
        except Exception as e:
            print(f"Error collecting predictions: {e}")
            return None
    
 