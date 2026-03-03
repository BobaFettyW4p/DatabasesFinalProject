"""
Query 5: Product View Counts (Popularity Ranking)
Neo4j implementation using graph traversal
"""

import psycopg2
import pymongo
from neo4j import GraphDatabase
from timing_utils import end_query_timer, start_query_timer

# Connect
pg_conn = psycopg2.connect(
    host='localhost', port=5432, database='ecommerce',
    user='postgres', password='postgres'
)

mongo_client = pymongo.MongoClient('localhost', 27017)
mongo_db = mongo_client['ecommerce']

neo4j_driver = GraphDatabase.driver(
    'bolt://localhost:7687',
    auth=('neo4j', 'your_password')
)

print("="*80)
print("QUERY 5: Products Ranked by View Count")
print("="*80)

query_start_time = start_query_timer()

# ============================================================================
# NEO4J GRAPH TRAVERSAL
# ============================================================================

print("\nNeo4j Cypher Query:")
print("-" * 80)

cypher_query = """
MATCH (u:User)-[v:VIEWED]->(p:Product)
RETURN p.product_id as product_id,
       p.name as name,
       p.category as category,
       p.price as price,
       COUNT(v) as view_count,
       COUNT(DISTINCT u) as unique_viewers
ORDER BY view_count DESC
"""

print(cypher_query)

final_results = []

try:
    with neo4j_driver.session() as session:
        result = session.run(cypher_query)
        
        for idx, record in enumerate(result, 1):
            final_results.append({
                'rank': idx,
                'name': record['name'],
                'category': record['category'] or 'N/A',
                'price': float(record['price']),
                'views': record['view_count'],
                'unique_viewers': record['unique_viewers']
            })
    
    print(f"\nNeo4j Result: {len(final_results)} products with view data")

except Exception as e:
    print(f"\nNeo4j query failed: {e}")
    print("\nFalling back to MongoDB + PostgreSQL approach...")
    
    # Fallback to MongoDB + PostgreSQL
    pipeline = [
        {'$match': {'event_type': 'view'}},
        {'$group': {
            '_id': '$event_data.product_id',
            'view_count': {'$sum': 1},
            'unique_viewers': {'$addToSet': '$user_id'}
        }},
        {'$project': {
            'product_id': '$_id',
            'view_count': 1,
            'unique_viewers': {'$size': '$unique_viewers'},
            '_id': 0
        }},
        {'$sort': {'view_count': -1}}
    ]
    
    mongo_results = list(mongo_db.user_events.aggregate(pipeline))
    print(f"MongoDB Result: {len(mongo_results)} products with view data")
    
    cursor = pg_conn.cursor()
    
    for idx, doc in enumerate(mongo_results, 1):
        product_id = doc.get('product_id', '')
        
        if 'item_' in product_id:
            item_id = int(product_id.replace('item_', ''))
        else:
            continue
        
        cursor.execute("""
            SELECT i.Name, i.BasePrice, c.CategoryName
            FROM Item i
            LEFT JOIN ItemCategory ic ON i.ItemID = ic.ItemID
            LEFT JOIN Category c ON ic.CategoryID = c.CategoryID
            WHERE i.ItemID = %s
            LIMIT 1
        """, (item_id,))
        
        product = cursor.fetchone()
        
        if product:
            name, price, category = product
            final_results.append({
                'rank': idx,
                'name': name,
                'category': category or 'N/A',
                'price': float(price),
                'views': doc['view_count'],
                'unique_viewers': doc['unique_viewers']
            })
    
    cursor.close()

# ============================================================================
# DISPLAY RESULTS
# ============================================================================

print("\n" + "="*80)
print("RESULTS: Products Ranked by Popularity")
print("="*80)

if final_results:
    print(f"\n{'#':<6} {'Product Name':<45} {'Views':<8} {'Viewers':<10}")
    print("-" * 80)
    
    for product in final_results[:20]:  # Show top 20
        rank_str = {1: '1st', 2: '2nd', 3: '3rd'}.get(product['rank'], f"{product['rank']:2d}th")
        name_short = product['name'][:42] + '...' if len(product['name']) > 45 else product['name']
        
        print(f"{rank_str:<6} {name_short:<45} {product['views']:<8} {product['unique_viewers']:<10}")
    
    if len(final_results) > 20:
        print(f"\n... and {len(final_results) - 20} more products")
    
    # Summary
    print("\n" + "="*80)
    print("Summary")
    print("="*80)
    
    total_views = sum(p['views'] for p in final_results)
    
    print(f"\nTotal Products: {len(final_results)}")
    print(f"Total Views: {total_views:,}")
    print(f"Average Views: {total_views / len(final_results):.2f} per product")
    
    if len(final_results) >= 3:
        print(f"\nTop 3 Most Popular:")
        for p in final_results[:3]:
            print(f"  {p['rank']}. {p['name']}: {p['views']} views")
    
    # Category breakdown
    category_views = {}
    for p in final_results:
        cat = p['category']
        category_views[cat] = category_views.get(cat, 0) + p['views']
    
    if category_views:
        print(f"\nViews by Category:")
        for cat, views in sorted(category_views.items(), key=lambda x: x[1], reverse=True):
            pct = (views / total_views) * 100
            print(f"  - {cat:<15}: {views:>6,} views ({pct:>5.1f}%)")

else:
    print("\nNo view data available")
    print("Run: python populate_databases.py --full")

print("\n" + "="*80)

# Clean up
pg_conn.close()
mongo_client.close()
neo4j_driver.close()
end_query_timer(query_start_time, "Query 5")