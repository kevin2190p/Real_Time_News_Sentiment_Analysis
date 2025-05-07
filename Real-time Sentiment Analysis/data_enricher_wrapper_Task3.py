from pyspark.sql import SparkSession, DataFrame
import logging
from Data_Validation_Task3 import DataValidator
from Data_Enricher_Task3 import DataEnricher
from config_Task3 import FINAL_COLUMN_STRUCTURE

class DataEnricherWrapper:
    """Wrapper for the DataValidator and DataEnricher classes."""
    
    def __init__(self, spark):
        """
        Initialize the data enricher wrapper.
        
        Args:
            spark (SparkSession): The Spark session
        """
        self.spark = spark
        self.logger = logging.getLogger(__name__)
    
    def process_data(self, input_df, output_path):
        """
        Process data through validation and enrichment pipeline.
        
        Args:
            input_df (DataFrame): Input DataFrame
            output_path (str): Path to save the enriched data
        """
        # Validate data
        validator = DataValidator(self.spark)
        validated_df = validator.set_dataframe(input_df) \
                      .run_all_validations() \
                      .get_clean_dataframe(drop_duplicates_url=False, 
                                          drop_duplicates_sentence=True,  
                                          filter_short_content=True)
        
        # Enrich data
        enricher = DataEnricher(self.spark)
        enriched_df = enricher.set_dataframe(validated_df) \
                            .run_all_enrichments(final_columns_order=FINAL_COLUMN_STRUCTURE)
        
        if enriched_df:
            print("\n--- Final Enriched Dataset (Sample Data) ---")
            enriched_df.show(5, truncate=True, vertical=True)
            
            print(f"\n--- Saving final enriched dataset to: {output_path} ---")
            
            
               
            enriched_df.coalesce(10).write.format("parquet").option("compression", "snappy").mode("overwrite").save(output_path)
        else:
            print("\nERROR: Enrichment process failed to produce a DataFrame.")
            return None
    
    