"""
Add Sample View Data for Query 2 Testing
Adds view history to Redis and MongoDB for a user named Sarah
"""

import psycopg2
import pymongo
import redis
from datetime import datetime, timedelta
import random
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

# ============================================================================
# CONNECTIONS
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
print("ADDING SAMPLE VIEW DATA FOR QUERY 2")
print("="*80)

# ============================================================================
# STEP 1: Find or create "Sarah"
# ============================================================================

print("\nStep 1: Finding/creating user 'Sarah'...")

pg_cursor = pg_conn.cursor()

# Try to find Sarah
pg_cursor.execute("""
    SELECT UserID, FirstName, LastName
    FROM "User"
    WHERE FirstName ILIKE '%Sarah%'
    LIMIT 1
""")

user_result = pg_cursor.fetchone()

if user_result:
    user_id = user_result[0]
    first_name = user_result[1]
    last_name = user_result[2]
    print(f"   Found existing user: {first_name} {last_name} (UserID: {user_id})")
else:
    # Create Sarah
    print("   Creating new user 'Sarah'...")
    
    # First create an address
    pg_cursor.execute("""
        INSERT INTO Address (AddressLine1, City, StateProvince, PostalCode, Country, CreatedAt)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING AddressID
    """, ("123 Main St", "San Francisco", "CA", "94102", "USA", datetime.now()))
    
    address_id = pg_cursor.fetchone()[0]
    
    # Create Sarah
    pg_cursor.execute("""
        INSERT INTO "User" (FirstName, LastName, Email, Username, PasswordHash, 
                           PrimaryAddressID, CreatedAt, UpdatedAt)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING UserID
    """, ("Sarah", "Johnson", "sarah.johnson@example.com", "sjohnson", 
          fake.sha256(), address_id, datetime.now(), datetime.now()))
    
    user_id = pg_cursor.fetchone()[0]
    pg_conn.commit()
    
    print(f"   Created user: Sarah Johnson (UserID: {user_id})")
    first_name = "Sarah"
    last_name = "Johnson"

# ============================================================================
# STEP 2: Get available products
# ============================================================================

print("\nStep 2: Getting available products...")

pg_cursor.execute("""
    SELECT ItemID, Name
    FROM Item
    WHERE IsActive = true
    ORDER BY ItemID
    LIMIT 20
""")

products = pg_cursor.fetchall()
print(f"   Found {len(products)} products")

# ============================================================================
# STEP 3: Generate view history (past 6 months)
# ============================================================================

print("\nStep 3: Generating view history...")

# Create 15 view events over past 6 months
view_events = []
now = datetime.now()

for i in range(15):
    # Random time in past 6 months
    days_ago = random.randint(1, 180)
    hours_ago = random.randint(0, 23)
    minutes_ago = random.randint(0, 59)
    
    viewed_at = now - timedelta(days=days_ago, hours=hours_ago, minutes=minutes_ago)
    
    # Random product
    product = random.choice(products)
    product_id = product[0]
    product_name = product[1]
    
    # Random view duration (5-300 seconds)
    duration = random.randint(5, 300)
    
    view_events.append({
        'product_id': product_id,
        'product_name': product_name,
        'viewed_at': viewed_at,
        'duration_seconds': duration
    })

# Sort by most recent first
view_events.sort(key=lambda x: x['viewed_at'], reverse=True)

print(f"   Generated {len(view_events)} view events")

# ============================================================================
# STEP 4: Add to MongoDB (all 15 events)
# ============================================================================

print("\nStep 4: Adding events to MongoDB...")

mongo_events = []

for event in view_events:
    mongo_event = {
        'user_id': f'user_{user_id}',
        'session_id': f'sess_{fake.uuid4()[:8]}',
        'event_type': 'view',
        'event_data': {
            'product_id': f'item_{event["product_id"]}',
            'duration_seconds': event['duration_seconds']
        },
        'timestamp': event['viewed_at'],
        'device_type': random.choice(['laptop', 'mobile', 'tablet']),
        'browser': random.choice(['Chrome', 'Firefox', 'Safari']),
        'created_at': datetime.now()
    }
    mongo_events.append(mongo_event)

if mongo_events:
    mongo_db.user_events.insert_many(mongo_events)
    print(f"   Inserted {len(mongo_events)} events into MongoDB")

# ============================================================================
# STEP 5: Add most recent 10 to Redis cache
# ============================================================================

print("\nStep 5: Caching most recent 10 views in Redis...")

redis_key = f"recent_views:{user_id}"

# Clear existing
redis_client.delete(redis_key)

# Add most recent 10 to Redis sorted set
for event in view_events[:10]:
    # Member: "product_id:duration"
    member = f"item_{event['product_id']}:{event['duration_seconds']}"
    # Score: timestamp (for sorting)
    score = event['viewed_at'].timestamp()
    
    redis_client.zadd(redis_key, {member: score})

# Set expiration (6 months)
redis_client.expire(redis_key, 15552000)  # 6 months in seconds

count = redis_client.zcard(redis_key)
print(f"   Cached {count} views in Redis")

# ============================================================================
# DISPLAY SUMMARY
# ============================================================================

print("\n" + "="*80)
print("SAMPLE DATA SUMMARY")
print("="*80)

print(f"\nUser: {first_name} {last_name} (ID: {user_id})")
print(f"View Events Generated: {len(view_events)}")
print(f"MongoDB Events: {len(mongo_events)}")
print(f"Redis Cached: {count}")

print("\nMost Recent 5 Views:")
for idx, event in enumerate(view_events[:5], 1):
    time_ago = now - event['viewed_at']
    if time_ago.days > 0:
        time_str = f"{time_ago.days} days ago"
    else:
        time_str = f"{time_ago.seconds // 3600} hours ago"
    
    print(f"{idx}. {event['product_name']}")
    print(f"   Viewed: {event['viewed_at'].strftime('%Y-%m-%d %H:%M')} ({time_str})")
    print(f"   Duration: {event['duration_seconds']} seconds")

print("\n" + "="*80)
print("Sample data added successfully!")
print("="*80)

# Clean up
pg_cursor.close()
pg_conn.close()
mongo_client.close()
redis_client.close()