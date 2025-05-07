from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, avg
from neo4j_connector_Task5 import Neo4jConnector

class Task5Processor:
    def __init__(self, parquet_path, neo4j_uri, neo4j_user, neo4j_password):
        # init Spark
        self.spark = SparkSession.builder.appName("Task5Processor").getOrCreate()
        self.parquet_path = parquet_path
        
        # init ONE Neo4jConnector instance
        self.connector = Neo4jConnector(neo4j_uri, neo4j_user, neo4j_password)
        self.df = None

    def load_data(self):
        self.df = self.spark.read.parquet(self.parquet_path)
        self.df.printSchema()
        print(f"Total rows in DataFrame: {self.df.count()}")

    def compute_article_sentiment(self):
        df_valid = self.df.filter(col("Sentiment_Result").isNotNull())
        df_avg = (
            df_valid
            .groupBy("url")
            .agg(avg("Sentiment_Result").alias("article_sentiment"))
        )
        # join back so every row carries the article_sentiment
        self.df = self.df.join(df_avg, on="url", how="left")
        print("✅ Article-level sentiment computed.")

    def process_posts(self):
        posts = (
            self.df
            .select(
                "url", 
                col("publishedAt"), 
                col("cleaned_title"), 
                col("cleaned_description"),
                col("word_count"), 
                col("article_sentiment")
            )
            .dropDuplicates(["url"])
            .rdd
            .map(lambda r: (
                "post_"+str(abs(hash(r.url))),
                r.publishedAt or "",
                r.url or "",
                r.cleaned_title or "",
                r.cleaned_description or "",
                int(r.word_count or 0),
                float(r.article_sentiment) if r.article_sentiment is not None else None
            ))
            .collect()
        )

        for post_id, pub, url, title, desc, wc, sent in posts:
            self.connector.create_post_node(
                post_id, pub, url, title, desc, wc, sent
            )
        print(f"✅ Created {len(posts)} Post nodes.")

    def process_person_nodes(self):
        people = (
            self.df
            .select(explode(col("people_mentioned")).alias("name"))
            .filter(col("name").isNotNull())
            .select(col("name"))
            .distinct()
            .rdd
            .map(lambda r: r.name.strip())
            .filter(lambda s: s != "")
            .collect()
        )
        for name in people:
            self.connector.create_person_node(name)
        print(f"✅ Created {len(people)} Person nodes.")

    def process_link_person_post(self):
        pairs = (
            self.df
            .select("url", explode(col("people_mentioned")).alias("person"))
            .filter(col("url").isNotNull() & col("person").isNotNull())
            .rdd
            .map(lambda r: (
                "post_"+str(abs(hash(r.url))),
                r.person.strip()
            ))
            .distinct()
            .filter(lambda x: x[1] != "")
            .collect()
        )
        for pid, person in pairs:
            self.connector.link_post_to_person(pid, person)
        print(f"✅ Linked {len(pairs)} Person→Post relationships.")

    def process_organizations(self):
        orgs = (
            self.df
            .select(explode(col("organizations_mentioned")).alias("name"))
            .filter(col("name").isNotNull())
            .distinct()
            .rdd.map(lambda r: r.name.strip())
            .filter(lambda s: s != "")
            .collect()
        )
        for name in orgs:
            self.connector.create_organization_node(name)
        print(f"✅ Created {len(orgs)} Organization nodes.")

    def process_link_organization_post(self):
        pairs = (
            self.df
            .select("url", explode(col("organizations_mentioned")).alias("org"))
            .filter(col("url").isNotNull() & col("org").isNotNull())
            .rdd.map(lambda r: (
                "post_"+str(abs(hash(r.url))),
                r.org.strip()
            ))
            .distinct()
            .filter(lambda x: x[1] != "")
            .collect()
        )
        for pid, org in pairs:
            self.connector.link_post_to_organization(pid, org)
        print(f"✅ Linked {len(pairs)} Organization→Post relationships.")

    def process_locations(self):
        locs = (
            self.df
            .select(explode(col("locations_mentioned")).alias("name"))
            .filter(col("name").isNotNull())
            .distinct()
            .rdd.map(lambda r: r.name.strip())
            .filter(lambda s: s != "")
            .collect()
        )
        for name in locs:
            self.connector.create_location_node(name)
        print(f"✅ Created {len(locs)} Location nodes.")

    def process_link_location_post(self):
        pairs = (
            self.df
            .select("url", explode(col("locations_mentioned")).alias("loc"))
            .filter(col("url").isNotNull() & col("loc").isNotNull())
            .rdd.map(lambda r: (
                "post_"+str(abs(hash(r.url))),
                r.loc.strip()
            ))
            .distinct()
            .filter(lambda x: x[1] != "")
            .collect()
        )
        for pid, loc in pairs:
            self.connector.link_post_to_location(pid, loc)
        print(f"✅ Linked {len(pairs)} Location→Post relationships.")

    def process_categories(self):
        cats = (
            self.df
            .select(col("category").alias("name"))
            .filter(col("name").isNotNull())
            .distinct()
            .rdd.map(lambda r: r.name.strip())
            .filter(lambda s: s != "")
            .collect()
        )
        for name in cats:
            self.connector.create_category_node(name)
        print(f"✅ Created {len(cats)} Category nodes.")

    def process_link_category_post(self):
        pairs = (
            self.df
            .select("url", col("category").alias("cat"))
            .filter(col("url").isNotNull() & col("cat").isNotNull())
            .rdd.map(lambda r: (
                "post_"+str(abs(hash(r.url))),
                r.cat.strip()
            ))
            .distinct()
            .filter(lambda x: x[1] != "")
            .collect()
        )
        for pid, cat in pairs:
            self.connector.link_post_to_category(pid, cat)
        print(f"✅ Linked {len(pairs)} Category→Post relationships.")

    def process_sentiment(self):
        def classify(v):
            if v is None: return "Unknown"
            if v > 0.1:   return "Positive"
            if v < -0.1:  return "Negative"
            return "Neutral"

        pairs = (
            self.df
            .select("url", col("article_sentiment"))
            .rdd.map(lambda r: (
                "post_"+str(abs(hash(r.url))),
                classify(r.article_sentiment)
            ))
            .distinct()
            .collect()
        )
        for _, sentiment in pairs:
            self.connector.create_sentiment_node(sentiment)
        for pid, sentiment in pairs:
            self.connector.link_post_to_sentiment(pid, sentiment)
        print(f"✅ Created & linked {len(pairs)} Sentiment→Post relationships.")

    def process_dates(self):
        pairs = (
            self.df
            .select("url", col("publishedAt").alias("pub"))
            .filter(col("url").isNotNull() & col("pub").isNotNull())
            .rdd
            .map(lambda r: (
                "post_" + str(abs(hash(r.url))),
                # strip off the 'T' and everything after
                (r.pub.strip().split("T", 1)[0] if "T" in r.pub else r.pub.strip())
            ))
            .distinct()
            .filter(lambda x: x[1] != "")
            .collect()
        )
        
        for _, pub in pairs:
            self.connector.create_date_node(pub)
        for pid, pub in pairs:
            self.connector.link_post_to_date(pid, pub, "PUBLISHED_ON")
        print(f"✅ Created & linked {len(pairs)} Date→Post relationships.")

    def process_sources(self):
        pairs = (
            self.df
            .select(
                "url",
                col("source").alias("name"),
                col("source_domain").alias("domain")
            )
            .filter(
                col("url").isNotNull() &
                col("name").isNotNull() &
                col("domain").isNotNull()
            )
            .distinct()
            .rdd.map(lambda r: (
                "post_" + str(abs(hash(r.url))),
                r.domain.strip(),
                r.name.strip()
            ))
            .filter(lambda x: x[1] != "" and x[2] != "")
            .collect()
        )

        for _, domain, name in pairs:
            self.connector.create_source_node(domain, name)
        for post_id, domain, _ in pairs:
            self.connector.link_post_to_source(post_id, domain)

        print(f"✅ Created & linked {len(pairs)} Source nodes.")


    def close(self):
        """Close Neo4j connection and stop Spark."""
        self.connector.close()
        self.spark.stop()
        print("🛑 Connector & Spark session closed.")