"""
Query 4: Fashion Products (Blue OR Large)
"""

import psycopg2
import pymongo
from timing_utils import end_query_timer, start_query_timer

# Connect
pg_conn = psycopg2.connect(
    host='localhost', port=5432, database='ecommerce',
    user='postgres', password='postgres'
)

mongo_client = pymongo.MongoClient('localhost', 27017)
mongo_db = mongo_client['ecommerce']

print("="*80)
print("QUERY 4: Fashion Products in Blue OR Large Size")
print("="*80)

query_start_time = start_query_timer()

# ============================================================================
# STEP 1: POSTGRESQL - Get Fashion Products
# ============================================================================

print("\n[STEP 1] PostgreSQL Query:")
print("-" * 80)

sql = """
SELECT 
    i.ItemID,
    i.Name,
    i.BasePrice,
    i.TotalStockQuantity
FROM Item i
JOIN ItemCategory ic ON i.ItemID = ic.ItemID
JOIN Category c ON ic.CategoryID = c.CategoryID
WHERE c.CategoryName = 'Fashion'
  AND i.IsActive = true;
"""

print(sql)

cursor = pg_conn.cursor()
cursor.execute(sql)
pg_results = cursor.fetchall()

print(f"PostgreSQL Result: {len(pg_results)} fashion products")

# ============================================================================
# STEP 2: MONGODB - Filter by Blue OR Large
# ============================================================================

print("\n[STEP 2] MongoDB Query:")
print("-" * 80)

mongo_query_display = """
db.product_attributes.find({
    category: 'fashion',
    $or: [
        { 'attributes.available_colors': { $in: ['blue'] } },
        { 'attributes.available_sizes': { $in: ['L', 'XL'] } }
    ]
})
"""

print(mongo_query_display)

# Execute MongoDB query
mongo_query = {
    'category': 'fashion',
    '$or': [
        {'attributes.available_colors': {'$in': ['blue']}},
        {'attributes.available_sizes': {'$in': ['L', 'XL']}}
    ]
}

mongo_results = list(mongo_db.product_attributes.find(mongo_query))

# Extract matching item IDs
matching_ids = set()
mongo_details = {}

for doc in mongo_results:
    product_id = doc.get('product_id', '')
    if 'item_' in product_id:
        item_id = int(product_id.replace('item_', ''))
        matching_ids.add(item_id)
        mongo_details[item_id] = doc

print(f"MongoDB Result: {len(matching_ids)} products match filter")

# ============================================================================
# STEP 3: COMBINE RESULTS
# ============================================================================

print("\n[STEP 3] Combined Results:")
print("-" * 80)

# Filter PostgreSQL results by MongoDB matches
final_products = []

for row in pg_results:
    item_id, name, price, stock = row
    
    if item_id in matching_ids:
        doc = mongo_details[item_id]
        attrs = doc.get('attributes', {})
        
        has_blue = 'blue' in attrs.get('available_colors', [])
        has_large = any(sz in ['L', 'XL'] for sz in attrs.get('available_sizes', []))
        
        final_products.append({
            'id': item_id,
            'name': name,
            'price': float(price),
            'stock': stock,
            'colors': attrs.get('available_colors', []),
            'sizes': attrs.get('available_sizes', []),
            'has_blue': has_blue,
            'has_large': has_large
        })

print(f"\nFinal Result: {len(final_products)} products (Fashion AND (Blue OR Large))")

# ============================================================================
# DISPLAY RESULTS
# ============================================================================

if final_products:
    print("\n" + "="*80)
    print(f"{'ID':<5} {'Name':<40} {'Price':<10} {'Match':<20}")
    print("="*80)
    
    for product in final_products:
        match = []
        if product['has_blue']:
            match.append('Blue')
        if product['has_large']:
            match.append('L/XL')
        
        match_str = ' + '.join(match)
        name_short = product['name'][:37] + '...' if len(product['name']) > 40 else product['name']
        
        print(f"{product['id']:<5} {name_short:<40} ${product['price']:<9.2f} {match_str:<20}")
    
    print("="*80)
    
    print("\nDetailed View")
    print("-" * 80)
    
    for product in final_products:
        print(f"\n{product['name']}")
        print(f"  Price: ${product['price']:.2f} | Stock: {product['stock']} units")
        print(f"  Colors: {', '.join(product['colors']) if product['colors'] else 'N/A'}")
        print(f"  Sizes: {', '.join(product['sizes']) if product['sizes'] else 'N/A'}")
    
    # Summary
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    
    blue_only = sum(1 for p in final_products if p['has_blue'] and not p['has_large'])
    large_only = sum(1 for p in final_products if p['has_large'] and not p['has_blue'])
    both = sum(1 for p in final_products if p['has_blue'] and p['has_large'])
    
    print(f"\nBlue only: {blue_only}")
    print(f"Large/XL only: {large_only}")
    print(f"Both: {both}")
    print(f"───────────────")
    print(f"Total: {len(final_products)}")

else:
    print("\nNo products found matching criteria")

print("\n" + "="*80)

# Clean up
cursor.close()
pg_conn.close()
mongo_client.close()
end_query_timer(query_start_time, "Query 4")
