from queries_Task5 import QueryHelper

def run_all_queries(neo4j_uri, neo4j_user, neo4j_password):
    qh = QueryHelper(neo4j_uri, neo4j_user, neo4j_password)

    print("\n📊 SENTIMENT DISTRIBUTION")
    print("-" * 30)
    print(f"{'Sentiment':<15} | {'Post(s)':>5}")
    print("-" * 30)
    for row in qh.post_count_by_sentiment():
        print(f"{row['sentiment']:<15} | {row['posts']:>5}")

    print("\n🧑 TOP 15 PEOPLE MENTIONED")
    print("-" * 45)
    print(f"{'Person':<30} | {'Mentions':>8}")
    print("-" * 45)
    for row in qh.top_people():
        print(f"{row['person']:<30} | {row['mentions']:>8}")

    print("\n🌐 TOP 10 SOURCES BY DOMAIN")
    print("-" * 45)
    print(f"{'Source':<30} | {'Post(s)':>8}")
    print("-" * 45)
    for row in qh.top_sources(10):
        print(f"{row['source']:<30} | {row['posts']:>8}")

    print("\n📅 DAILY POST COUNT")
    print("-" * 30)
    print(f"{'Date':<15} | {'Post(s)':>8}")
    print("-" * 30)
    for row in qh.daily_post_count():
        print(f"{row['date']:<15} | {row['posts']:>8}")

    print("\n📚 TOP 10 CATEGORIES BY POST COUNT")
    print("-" * 50)
    print(f"{'Category':<30} | {'Post(s)':>8}")
    print("-" * 50)
    for row in qh.top_categories(10):
        print(f"{row['category']:<30} | {row['post_count']:>8}")

    print("\n📍 TOP 10 LOCATIONS MENTIONED")
    print("-" * 50)
    print(f"{'Location':<30} | {'Mentions':>8}")
    print("-" * 50)
    for row in qh.top_locations(10):
        print(f"{row['location']:<30} | {row['mentions']:>8}")

    print("\n📘 SENTIMENT BREAKDOWN BY CATEGORY")
    print("-" * 70)
    print(f"{'Category':<35} | {'Sentiment':<10} | {'Post(s)':>8}")
    print("-" * 70)
    for row in qh.sentiment_by_category():
        print(f"{row['category']:<35} | {row['sentiment']:<10} | {row['count']:>8}")

    print("\n🔗 CO-MENTIONS: TOP PEOPLE & ORGANIZATIONS")
    print("-" * 100)
    print(f"{'Person':<25} ↔ {'Organization':<60} | {'Times':>6}")
    print("-" * 100)
    for row in qh.orgs_by_top_people()[:15]:
        print(f"{row['person']:<25} ↔ {row['organization']:<60} | {row['co_mentions']:>6}")

    print("\n📈 SENTIMENT SUMMARY BY CATEGORY")
    print("-" * 65)
    print(f"{'Sentiment':<12} | {'Category':<35} | {'Post(s)':>8}")
    print("-" * 65)
    for row in qh.top_categories_by_sentiment()[:15]:
        print(f"{row['sentiment']:<12} | {row['category']:<35} | {row['count']:>8}")

    print("\n📆 DAILY SENTIMENT SUMMARY")
    print("-" * 40)
    print(f"{'Date':<15} | {'Sentiment':<10} | {'Post(s)':>8}")
    print("-" * 40)
    for row in qh.daily_sentiment_summary():
        print(f"{row['date']:<15} | {row['sentiment']:<10} | {row['count']:>8}")


    qh.close()
    print("\n✅ All queries finished.")
