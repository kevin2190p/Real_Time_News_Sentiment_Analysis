# queries.py
# -------------
from neo4j import GraphDatabase
from typing import List, Dict, Any


class QueryHelper:
    def __init__(self, uri: str, user: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    # -------------  generic helpers -------------
    def _run(self, cypher: str, **params) -> List[Dict[str, Any]]:
        with self.driver.session() as sess:
            recs = sess.run(cypher, **params)
            return [dict(r) for r in recs]

    def close(self):
        self.driver.close()

    # Sentiment distribution
    def post_count_by_sentiment(self) -> List[Dict[str, Any]]:
        cypher = """
        MATCH (p:Post)-[:HAS_SENTIMENT]->(s:Sentiment)
        RETURN s.category AS sentiment, count(*) AS posts
        ORDER BY posts DESC
        """
        return self._run(cypher)

    # Top 15 most mentioned people
    def top_people(self, top_n: int = 15) -> List[Dict[str, Any]]:
        cypher = """
        MATCH (pe:Person)-[:MENTIONED_IN_PERSON]->(p:Post)
        RETURN pe.name AS person, count(*) AS mentions
        ORDER BY mentions DESC
        LIMIT $n
        """
        return self._run(cypher, n=top_n)

    # Top 10 sources domain by number of articles
    def top_sources(self, top_n: int = 10) -> List[Dict[str, Any]]:
        cypher = """
        MATCH (so:Source)<-[:PUBLISHED_BY]-(p:Post)
        RETURN so.domain AS source , count(*) AS posts
        ORDER BY posts DESC
        LIMIT $n
        """
        return self._run(cypher, n=top_n)

    # Article count by date
    def daily_post_count(self) -> List[Dict[str, Any]]:
        cypher = """
        MATCH (p:Post)-[:PUBLISHED_ON]->(d:Date)
        WITH d.value AS date , count(*) AS posts
        RETURN date , posts
        ORDER BY date
        """
        return self._run(cypher)

    # Top categories by post count
    def top_categories(self, top_n: int = 10) -> List[Dict[str, Any]]:
        cypher = """
        MATCH (c:Category)<-[:BELONGS_TO]-(p:Post)
        RETURN c.name AS category, COUNT(p) AS post_count
        ORDER BY post_count DESC
        LIMIT $n
        """
        return self._run(cypher, n=top_n)

    # Most mentioned locations
    def top_locations(self, top_n: int = 10) -> List[Dict[str, Any]]:
        cypher = """
        MATCH (l:Location)-[:MENTIONED_IN_LOCATION]->(p:Post)
        RETURN l.name AS location, COUNT(p) AS mentions
        ORDER BY mentions DESC
        LIMIT $n
        """
        return self._run(cypher, n=top_n)

    # Sentiment breakdown per category
    def sentiment_by_category(self) -> List[Dict[str, Any]]:
        cypher = """
        MATCH (p:Post)-[:BELONGS_TO]->(c:Category),
              (p)-[:HAS_SENTIMENT]->(s:Sentiment)
        RETURN c.name AS category, s.category AS sentiment, COUNT(*) AS count
        ORDER BY category, sentiment
        """
        return self._run(cypher)

    # Number of articles per source domain
    def posts_by_source(self) -> List[Dict[str, Any]]:
        cypher = """
        MATCH (p:Post)-[:PUBLISHED_BY]->(s:Source)
        RETURN s.domain AS source, COUNT(*) AS posts
        ORDER BY posts DESC
        """
        return self._run(cypher)

    # Top Organizations by Person Mentions
    def orgs_by_top_people(self) -> List[Dict[str, Any]]:
        cypher = """
        MATCH (p:Person)-[:MENTIONED_IN_PERSON]->(post:Post)<-[:MENTIONED_IN_ORG]-(o:Organization)
        RETURN p.name AS person, o.name AS organization, COUNT(*) AS co_mentions
        ORDER BY person, co_mentions DESC
        """
        return self._run(cypher)

    # Top Categories per Sentiment
    def top_categories_by_sentiment(self) -> List[Dict[str, Any]]:
        cypher = """
        MATCH (p:Post)-[:BELONGS_TO]->(c:Category),
              (p)-[:HAS_SENTIMENT]->(s:Sentiment)
        RETURN s.category AS sentiment, c.name AS category, COUNT(*) AS count
        ORDER BY sentiment, count DESC
        """
        return self._run(cypher)

    # Top Sources per Organization
    def top_sources_by_organization(self) -> List[Dict[str, Any]]:
        cypher = """
        MATCH (o:Organization)-[:MENTIONED_IN_ORG]->(p:Post)-[:FROM_SOURCE]->(s:Source)
        RETURN o.name AS organization, s.domain AS source_domain, COUNT(*) AS mentions
        ORDER BY organization, mentions DESC
        """
        return self._run(cypher)

    # Daily Sentiment Summary
    def daily_sentiment_summary(self) -> List[Dict[str, Any]]:
        cypher = """
        MATCH (p:Post)-[:PUBLISHED_ON]->(d:Date),
              (p)-[:HAS_SENTIMENT]->(s:Sentiment)
        RETURN d.value AS date, s.category AS sentiment, COUNT(*) AS count
        ORDER BY date ASC
        """
        return self._run(cypher)
