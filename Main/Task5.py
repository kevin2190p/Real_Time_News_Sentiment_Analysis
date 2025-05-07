from neo4j_connector_Task5 import Neo4jConnector
from processor_Task5 import Task5Processor
from queries_Task5 import QueryHelper 
from query_runner_Task5 import run_all_queries

# Neo4j connection parameters
neo4j_uri = "neo4j+s://52880ed1.databases.neo4j.io"
neo4j_user = "neo4j"
neo4j_password = "9Zl86l-LUkWvPaN7scExzItvE2vNuH8SEeRF7iUj6OI"
parquet_path = "Enriched_With_Date.parquet"

if __name__ == "__main__":
    # 1) Create unique constraints once to prevent duplicate nodes
    print("-- Ensuring Neo4j uniqueness constraints...")
    Neo4jConnector.ensure_constraints(neo4j_uri, neo4j_user, neo4j_password)

    # 2) Initialize processor and load data
    processor = Task5Processor(parquet_path, neo4j_uri, neo4j_user, neo4j_password)
    processor.load_data()

    # 3) Compute article-level sentiment
    print("-- Computing article-level sentiment...")
    processor.compute_article_sentiment()

    # 4) Create nodes & relationships
    print("-- Creating Post nodes...")
    processor.process_posts()

    print("-- Creating and linking Person nodes...")
    processor.process_person_nodes()
    processor.process_link_person_post()

    print("-- Creating and linking Organization nodes...")
    processor.process_organizations()
    processor.process_link_organization_post()

    print("-- Creating and linking Location nodes...")
    processor.process_locations()
    processor.process_link_location_post()

    print("-- Creating and linking Category nodes...")
    processor.process_categories()
    processor.process_link_category_post()

    print("-- Creating and linking Sentiment nodes...")
    processor.process_sentiment()

    print("-- Creating and linking Date nodes...")
    processor.process_dates()

    print("-- Creating and linking Source nodes...")
    processor.process_sources()
    processor.connector.normalize_source_names()

    # 5) Clean up
    processor.close()
    print("All data successfully pushed to Neo4j!")

    # 6) Sample analytical queries
    print("== Running sample analytical queries ==")
    run_all_queries(neo4j_uri, neo4j_user, neo4j_password)


