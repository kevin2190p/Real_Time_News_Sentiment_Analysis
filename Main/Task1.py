import sys
import logging
from pipeline_Task1 import NewsPipeline
from config_Task1 import CONFIG

# Configure basic logging for your application (optional, but good practice)
logging.basicConfig(level=logging.WARNING, format='%(levelname)s: %(message)s')

# Configure logging level for py4j to suppress INFO messages
logging.getLogger("py4j").setLevel(logging.WARNING)

# Configure logging level for kafka-python to suppress INFO messages
logging.getLogger("kafka").setLevel(logging.WARNING)

if __name__ == "__main__":
    print("=====================================================")
    print(" Initializing News Processing Pipeline (Distributed) ")
    print("=====================================================")

    try:
        pipeline = NewsPipeline(config=CONFIG)

        # Run the pipeline
        pipeline.run()

    except Exception as e:
        print(f"ERROR: Main script encountered an error: {e}", file=sys.stderr)
        sys.exit(1)

    print("=============================")
    print(" Pipeline Execution Complete ")
    print("=============================")