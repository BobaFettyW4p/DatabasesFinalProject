"""
Query 11: Cart Abandonment Rate
"""

import psycopg2
import redis
from datetime import datetime, timedelta

# Connect
pg_conn = psycopg2.connect(
    host='localhost', port=5432, database='ecommerce',
    user='postgres', password='postgres'
)

redis_client = redis.Redis(
    host='localhost', port=6379, db=0, decode_responses=True
)

print("="*80)
print("QUERY 11: Cart Abandonment Rate (Past 30 Days)")
print("="*80)

cursor = pg_conn.cursor()

# Date threshold
thirty_days_ago = datetime.now() - timedelta(days=30)

# Get carts from PostgreSQL
cursor.execute("""
    SELECT Status, COUNT(*)
    FROM ShoppingCart
    WHERE CreatedAt >= %s
    GROUP BY Status
""", (thirty_days_ago,))

pg_status_counts = dict(cursor.fetchall())

# Get active carts from Redis
cart_keys = list(redis_client.scan_iter("cart:*:*"))

redis_active_count = 0
for cart_key in cart_keys:
    cart_data = redis_client.hgetall(cart_key)
    if cart_data.get('created_at'):
        try:
            created_at = datetime.fromisoformat(cart_data['created_at'])
            if created_at >= thirty_days_ago:
                redis_active_count += 1
        except ValueError:
            continue

# Calculate totals
pg_active = pg_status_counts.get('active', 0)
pg_abandoned = pg_status_counts.get('abandoned', 0)
pg_converted = pg_status_counts.get('converted', 0)

total_active = pg_active + redis_active_count
total_abandoned = pg_abandoned
total_converted = pg_converted
total_carts = total_active + total_abandoned + total_converted

# Calculate percentage
if total_carts > 0:
    non_converted = total_active + total_abandoned
    abandonment_rate = (non_converted / total_carts) * 100
else:
    abandonment_rate = 0
    total_carts = 0
    non_converted = 0

# Display result
print("\n" + "="*80)
print("RESULT")
print("="*80)

print(f"\nTotal Carts (Past 30 Days): {total_carts}")
print(f"  Converted to Orders: {total_converted}")
print(f"  Did NOT Convert: {non_converted}")

print(f"\nCart Abandonment Rate: {abandonment_rate:.1f}%")

print("\n" + "="*80)

# Clean up
cursor.close()
pg_conn.close()
redis_client.close()