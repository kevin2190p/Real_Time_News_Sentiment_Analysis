from neo4j import GraphDatabase

class Neo4jConnector:
    def __init__(self, uri, user, password):
        self.uri = uri
        self.user = user
        self.password = password
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
    
    def close(self):
        self.driver.close()

    @staticmethod
    def ensure_constraints(uri, user, password):
        """
        Create uniqueness constraints for all relevant labels.
        Call once at startup to avoid duplicate nodes under concurrency.
        """
        constraints = {
            "Sentiment": "category",
            "Person": "name",
            "Organization": "name",
            "Location": "name",
            "Category": "name",
            "Date": "value",
            "Source": "domain"
        }

        driver = GraphDatabase.driver(uri, auth=(user, password))
        with driver.session() as session:
            for label, property in constraints.items():
                query = f"""
                CREATE CONSTRAINT IF NOT EXISTS
                FOR (n:{label}) REQUIRE (n.{property}) IS UNIQUE
                """
                session.run(query)
        driver.close()

    # Internal helper for normalization
    @staticmethod
    def _normalize(raw: str) -> str:
        return raw.strip()

    def create_post_node(self, post_id, publishedAt, url, cleaned_title, cleaned_description, word_count, sentiment):
        query = """
        MERGE (p:Post {id: $post_id})
        SET p.url = $url,
            p.title = $cleaned_title,
            p.description = $cleaned_description,
            p.word_count = $word_count,
            p.sentiment = $sentiment
        """
        with self.driver.session() as session:
            session.execute_write(lambda tx: tx.run(query,
                                                      post_id=post_id,
                                                      url=url,
                                                      cleaned_title=cleaned_title,
                                                      cleaned_description=cleaned_description,
                                                      word_count=word_count,
                                                      sentiment=sentiment))
    
    def create_person_node(self, person: str):
        name = self._normalize(person)
        query = "MERGE (p:Person {name: $name})"
        with self.driver.session() as session:
            session.execute_write(lambda tx: tx.run(query, name=name))

    def link_post_to_person(self, post_id: str, person: str):
        name = self._normalize(person)
        query = (
            "MATCH (p:Post {id: $post_id}), (pe:Person {name: $name}) \n"
            "MERGE (pe)-[:MENTIONED_IN_PERSON]->(p)"
        )
        with self.driver.session() as session:
            session.execute_write(lambda tx: tx.run(query, post_id=post_id, name=name))
    
    def create_organization_node(self, org: str):
        name = self._normalize(org)
        query = "MERGE (o:Organization {name: $name})"
        with self.driver.session() as session:
            session.execute_write(lambda tx: tx.run(query, name=name))

    def link_post_to_organization(self, post_id: str, org: str):
        name = self._normalize(org)
        query = (
            "MATCH (p:Post {id: $post_id}), (o:Organization {name: $name}) \n"
            "MERGE (o)-[:MENTIONED_IN_ORG]->(p)"
        )
        with self.driver.session() as session:
            session.execute_write(lambda tx: tx.run(query, post_id=post_id, name=name))
    
    def create_location_node(self, loc: str):
        name = self._normalize(loc)
        query = "MERGE (l:Location {name: $name})"
        with self.driver.session() as session:
            session.execute_write(lambda tx: tx.run(query, name=name))

    def link_post_to_location(self, post_id: str, loc: str):
        name = self._normalize(loc)
        query = (
            "MATCH (p:Post {id: $post_id}), (l:Location {name: $name}) \n"
            "MERGE (l)-[:MENTIONED_IN_LOCATION]->(p)"
        )
        with self.driver.session() as session:
            session.execute_write(lambda tx: tx.run(query, post_id=post_id, name=name))
    
    # NEW METHODS FOR CATEGORY
    
    def create_category_node(self, category: str):
        name = self._normalize(category)
        query = "MERGE (c:Category {name: $name})"
        with self.driver.session() as session:
            session.execute_write(lambda tx: tx.run(query, name=name))

    def link_post_to_category(self, post_id: str, category: str):
        name = self._normalize(category)
        query = (
            "MATCH (p:Post {id: $post_id}), (c:Category {name: $name}) \n"
            "MERGE (p)-[:BELONGS_TO]->(c)"
        )
        with self.driver.session() as session:
            session.execute_write(lambda tx: tx.run(query, post_id=post_id, name=name))

    def create_sentiment_node(self, sentiment_category: str):
        """MERGE a single Sentiment node by category."""
        cat = self._normalize(sentiment_category)
        query = "MERGE (s:Sentiment {category: $cat})"
        with self.driver.session() as session:
            session.execute_write(lambda tx: tx.run(query, cat=cat))

    def link_post_to_sentiment(self, post_id: str, sentiment_category: str):
        """MERGE relationship between Post and Sentiment."""
        cat = self._normalize(sentiment_category)
        query = (
            "MATCH (p:Post {id: $post_id}), (s:Sentiment {category: $cat}) \n"
            "MERGE (p)-[:HAS_SENTIMENT]->(s)"
        )
        with self.driver.session() as session:
            session.execute_write(lambda tx: tx.run(query, post_id=post_id, cat=cat))

    def create_date_node(self, date_value: str):
        val = self._normalize(date_value)
        query = "MERGE (d:Date {value: $val})"
        with self.driver.session() as session:
            session.execute_write(lambda tx: tx.run(query, val=val))

    def link_post_to_date(self, post_id: str, date_value: str, relationship_type: str = "PUBLISHED_ON"):
        val = self._normalize(date_value)
        # MERGE dynamic relationship type
        query = (
            f"MATCH (p:Post {{id: $post_id}}), (d:Date {{value: $val}}) \n"
            f"MERGE (p)-[:{relationship_type}]->(d)"
        )
        with self.driver.session() as session:
            session.execute_write(lambda tx: tx.run(query, post_id=post_id, val=val))

    def create_source_node(self, domain: str, name: str):
        query = """
        MERGE (s:Source {domain: $domain})
        SET s.name = $name
        """
        with self.driver.session() as session:
            session.write_transaction(lambda tx: tx.run(query, domain=domain, name=name))

    def link_post_to_source(self, post_id: str, domain: str):
        query = """
        MATCH (p:Post {id: $post_id})
        MATCH (s:Source {domain: $domain})
        MERGE (p)-[:PUBLISHED_BY]->(s)
        """
        with self.driver.session() as session:
            session.write_transaction(lambda tx: tx.run(query, post_id=post_id, domain=domain))


    def normalize_source_names(self):
        query = """
        MATCH (s:Source)
        WHERE s.name = 'Other'
        WITH s, replace(s.domain, 'www.', '') AS stripped
        WITH s, split(stripped, '.')[0] AS shortName
        SET s.name = shortName
        """
        with self.driver.session() as session:
            session.run(query)