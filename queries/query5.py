"""
Query 5: Product View Counts (Popularity Ranking)
"""

import psycopg2
import pymongo

# Connect
pg_conn = psycopg2.connect(
    host='localhost', port=5432, database='ecommerce',
    user='postgres', password='postgres'
)

mongo_client = pymongo.MongoClient('localhost', 27017)
mongo_db = mongo_client['ecommerce']

print("="*80)
print("QUERY 5: Products Ranked by View Count")
print("="*80)

# ============================================================================
# MONGODB AGGREGATION
# ============================================================================

print("\nMongoDB Aggregation:")
print("-" * 80)

print("""
db.user_events.aggregate([
    { $match: { event_type: 'view' } },
    { $group: {
        _id: '$event_data.product_id',
        view_count: { $sum: 1 },
        unique_viewers: { $addToSet: '$user_id' }
    }},
    { $project: {
        product_id: '$_id',
        view_count: 1,
        unique_viewers: { $size: '$unique_viewers' }
    }},
    { $sort: { view_count: -1 } }
])
""")

# Execute
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

results = list(mongo_db.user_events.aggregate(pipeline))

print(f"MongoDB Result: {len(results)} products with view data")

# ============================================================================
# ENRICH WITH POSTGRESQL
# ============================================================================

print("\nEnriching with PostgreSQL product details...")

cursor = pg_conn.cursor()
final_results = []

for idx, doc in enumerate(results, 1):
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

# ============================================================================
# DISPLAY RESULTS
# ============================================================================

print("\n" + "="*80)
print("RESULTS: Products Ranked by Popularity")
print("="*80)

if final_results:
    print(f"\n{'#':<4} {'Product Name':<45} {'Views':<8} {'Viewers':<10}")
    print("-" * 80)
    
    for product in final_results:
        rank_str = {1: 'First', 2: 'Second', 3: 'Third'}.get(product['rank'], f"{product['rank']:2d}")
        name_short = product['name'][:42] + '...' if len(product['name']) > 45 else product['name']
        
        print(f"{rank_str:<4} {name_short:<45} {product['views']:<8} {product['unique_viewers']:<10}")
    
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
            print(f"  • {cat:<15}: {views:>4} views ({pct:>5.1f}%)")

else:
    print("\nNo view data available")
    print("Run: python add_sample_views.py to generate view data")

print("\n" + "="*80)

# Clean up
cursor.close()
pg_conn.close()
mongo_client.close()