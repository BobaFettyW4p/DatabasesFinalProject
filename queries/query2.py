"""
Query 2: Retrieve last 5 products viewed by a user within past 6 months
"""

import psycopg2
import pymongo
import redis
from datetime import datetime, timedelta
import json

# ============================================================================
# DATABASE CONNECTIONS
# ============================================================================

pg_conn = psycopg2.connect(
    host='localhost',
    port=5432,
    database='ecommerce',
    user='postgres',
    password='postgres'
)

mongo_client = pymongo.MongoClient('localhost', 27017)
mongo_db = mongo_client['ecommerce']

redis_client = redis.Redis(
    host='localhost',
    port=6379,
    db=0,
    decode_responses=True
)

print("="*80)
print("QUERY 2: Last 5 Products Viewed by User (Past 6 Months)")
print("="*80)

# ============================================================================
# STEP 1: Find the user ID for "Sarah" (or use user_id directly)
# ============================================================================

print("\nStep 1: Finding user 'Sarah'...")

pg_cursor = pg_conn.cursor()

# Look for a user named Sarah
pg_cursor.execute("""
    SELECT UserID, FirstName, LastName, Email
    FROM "User"
    WHERE FirstName ILIKE '%Sarah%'
    LIMIT 1
""")

user_result = pg_cursor.fetchone()

if user_result:
    user_id = user_result[0]
    first_name = user_result[1]
    last_name = user_result[2]
    email = user_result[3]
    print(f"   Found user: {first_name} {last_name} (UserID: {user_id}, Email: {email})")
else:
    # Fallback: use first user in database
    print("    No user named 'Sarah' found, using first user instead...")
    pg_cursor.execute("""
        SELECT UserID, FirstName, LastName, Email
        FROM "User"
        LIMIT 1
    """)
    user_result = pg_cursor.fetchone()
    user_id = user_result[0]
    first_name = user_result[1]
    last_name = user_result[2]
    email = user_result[3]
    print(f"   Using user: {first_name} {last_name} (UserID: {user_id})")

# ============================================================================
# STEP 2: Try to get recently viewed products from Redis (FAST PATH)
# ============================================================================

print("\nStep 2: Checking Redis cache for recent views...")

redis_key = f"recent_views:{user_id}"
recent_views = []

try:
    # Redis stores as Sorted Set with timestamp as score
    # ZREVRANGE gets in descending order (most recent first)
    redis_results = redis_client.zrevrange(redis_key, 0, 4, withscores=True)
    
    if redis_results:
        print(f"   Found {len(redis_results)} recent views in Redis cache")
        
        for member, score in redis_results:
            # member format: "product_id:duration_seconds"
            parts = member.split(':')
            product_id = parts[0]
            duration = int(parts[1]) if len(parts) > 1 else 0
            timestamp = datetime.fromtimestamp(score)
            
            recent_views.append({
                'product_id': product_id,
                'viewed_at': timestamp,
                'duration_seconds': duration,
                'source': 'Redis'
            })
    else:
        print("    No recent views found in Redis cache")
        
except Exception as e:
    print(f"    Redis error: {e}")

# ============================================================================
# STEP 3: If Redis has insufficient data, query MongoDB (FALLBACK)
# ============================================================================

if len(recent_views) < 5:
    print(f"\nStep 3: Querying MongoDB for additional view history...")
    print(f"   (Need {5 - len(recent_views)} more views)")
    
    # Calculate 6 months ago
    six_months_ago = datetime.now() - timedelta(days=180)
    
    # Query MongoDB for user_events of type "view"
    mongo_query = {
        'user_id': f'user_{user_id}',
        'event_type': 'view',
        'timestamp': {'$gte': six_months_ago}
    }
    
    mongo_results = mongo_db.user_events.find(mongo_query).sort('timestamp', -1).limit(5)
    
    mongo_views = []
    for event in mongo_results:
        product_id = event.get('event_data', {}).get('product_id', 'unknown')
        viewed_at = event.get('timestamp', datetime.now())
        duration = event.get('event_data', {}).get('duration_seconds', 0)
        
        mongo_views.append({
            'product_id': product_id,
            'viewed_at': viewed_at,
            'duration_seconds': duration,
            'source': 'MongoDB'
        })
    
    print(f"   Found {len(mongo_views)} views in MongoDB")
    
    # Combine and deduplicate
    # Prefer Redis data (more recent) over MongoDB
    existing_product_ids = {v['product_id'] for v in recent_views}
    
    for mongo_view in mongo_views:
        if mongo_view['product_id'] not in existing_product_ids and len(recent_views) < 5:
            recent_views.append(mongo_view)
            existing_product_ids.add(mongo_view['product_id'])

# ============================================================================
# STEP 4: Enrich with product details from PostgreSQL
# ============================================================================

print("\nStep 4: Enriching with product details from PostgreSQL...")

final_results = []

for view in recent_views[:5]:  # Limit to 5
    # Extract item_id from product_id (format: "item_123")
    product_id_str = view['product_id']
    
    try:
        # Extract numeric ID
        if product_id_str.startswith('item_'):
            item_id = int(product_id_str.replace('item_', ''))
        else:
            item_id = int(product_id_str)
        
        # Get product details from PostgreSQL
        pg_cursor.execute("""
            SELECT 
                i.ItemID,
                i.Name,
                i.Description,
                i.BasePrice,
                i.TotalStockQuantity,
                i.ImageURL,
                c.CategoryName
            FROM Item i
            LEFT JOIN ItemCategory ic ON i.ItemID = ic.ItemID
            LEFT JOIN Category c ON ic.CategoryID = c.CategoryID
            WHERE i.ItemID = %s
            LIMIT 1
        """, (item_id,))
        
        product = pg_cursor.fetchone()
        
        if product:
            final_results.append({
                'item_id': product[0],
                'name': product[1],
                'description': product[2],
                'price': float(product[3]),
                'stock': product[4],
                'image_url': product[5],
                'category': product[6] if product[6] else 'Uncategorized',
                'viewed_at': view['viewed_at'],
                'duration_seconds': view['duration_seconds'],
                'data_source': view['source']
            })
        else:
            # Product not found in PostgreSQL
            final_results.append({
                'item_id': item_id,
                'name': f'Unknown Product (ID: {item_id})',
                'description': 'Product details not available',
                'price': 0.0,
                'stock': 0,
                'image_url': None,
                'category': 'Unknown',
                'viewed_at': view['viewed_at'],
                'duration_seconds': view['duration_seconds'],
                'data_source': view['source']
            })
    
    except (ValueError, IndexError) as e:
        print(f"    Error processing {product_id_str}: {e}")
        continue

# ============================================================================
# DISPLAY RESULTS
# ============================================================================

print("\n" + "="*80)
print(f"RESULTS: Last {len(final_results)} Products Viewed by {first_name} {last_name}")
print("="*80)

if final_results:
    for idx, product in enumerate(final_results, 1):
        time_ago = datetime.now() - product['viewed_at']
        
        if time_ago.days > 0:
            time_str = f"{time_ago.days} days ago"
        elif time_ago.seconds // 3600 > 0:
            time_str = f"{time_ago.seconds // 3600} hours ago"
        else:
            time_str = f"{time_ago.seconds // 60} minutes ago"
        
        print(f"\n{idx}. {product['name']}")
        print(f"   Category: {product['category']}")
        print(f"   Price: ${product['price']:.2f}")
        print(f"   Viewed: {product['viewed_at'].strftime('%Y-%m-%d %H:%M:%S')} ({time_str})")
        print(f"   Time spent: {product['duration_seconds']} seconds")
        print(f"   Data Source: {product['data_source']}")
else:
    print("\n No recently viewed products found for this user")

# ============================================================================
# SUMMARY
# ============================================================================

print("\n" + "="*80)
print("QUERY EXECUTION SUMMARY")
print("="*80)

redis_count = sum(1 for r in final_results if r['data_source'] == 'Redis')
mongo_count = sum(1 for r in final_results if r['data_source'] == 'MongoDB')

print(f"\nUser: {first_name} {last_name} (ID: {user_id})")
print(f"Total Products Viewed: {len(final_results)}")
print(f"Time Range: Past 6 months")
print(f"\nData Sources:")
print(f"  - Redis (cache): {redis_count} products")
print(f"  - MongoDB (fallback): {mongo_count} products")
print(f"  - PostgreSQL (enrichment): {len(final_results)} products")

print("="*80)

# Clean up
pg_cursor.close()
pg_conn.close()
mongo_client.close()
redis_client.close()

print("\nQuery complete!")