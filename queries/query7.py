"""
Query 7: Cart Information - Device Type, Items, Amount
"""

import psycopg2
import redis
import json

# Connect
pg_conn = psycopg2.connect(
    host='localhost', port=5432, database='ecommerce',
    user='postgres', password='postgres'
)

redis_client = redis.Redis(
    host='localhost', port=6379, db=0, decode_responses=True
)

print("="*80)
print("QUERY 7: Shopping Cart Information")
print("="*80)

# ============================================================================
# REDIS - Active Carts
# ============================================================================

print("\n[REDIS] Fetching active carts...")

redis_carts = []
cart_keys = list(redis_client.scan_iter("cart:*:*"))

for cart_key in cart_keys:
    cart_data = redis_client.hgetall(cart_key)
    
    if cart_data:
        redis_carts.append({
            'source': 'Redis',
            'cart_id': cart_data.get('cart_id', 'unknown'),
            'user_id': cart_data.get('user_id', 'unknown'),
            'device_type': cart_data.get('device_type', 'unknown'),
            'total_items': int(cart_data.get('total_items', 0)),
            'total_amount': float(cart_data.get('total_amount', 0.0)),
            'status': 'active'
        })

print(f"Redis Result: {len(redis_carts)} active carts")

# ============================================================================
# POSTGRESQL - All Carts (Backup + Abandoned)
# ============================================================================

print("\n[POSTGRESQL] Fetching carts...")

cursor = pg_conn.cursor()

cursor.execute("""
    SELECT 
        ShoppingCartID,
        UserID,
        Status,
        TotalAmount,
        TotalItems,
        UpdatedAt
    FROM ShoppingCart
    ORDER BY UpdatedAt DESC
""")

pg_rows = cursor.fetchall()

pg_carts = []

for row in pg_rows:
    cart_id, user_id, status, amount, items, updated = row
    
    pg_carts.append({
        'source': 'PostgreSQL',
        'cart_id': f'pg_{cart_id}',
        'user_id': str(user_id),
        'device_type': 'unknown',  # Not stored in PostgreSQL
        'total_items': items,
        'total_amount': float(amount),
        'status': status
    })

print(f"PostgreSQL Result: {len(pg_rows)} carts")

# ============================================================================
# COMBINE RESULTS
# ============================================================================

all_carts = redis_carts + pg_carts

print(f"\nTotal Carts: {len(all_carts)}")

# ============================================================================
# DISPLAY RESULTS
# ============================================================================

print("\n" + "="*80)
print("CART INFORMATION")
print("="*80)

if all_carts:
    print(f"\n{'Source':<12} {'User':<8} {'Device':<10} {'Items':<7} {'Amount':<12} {'Status':<10}")
    print("-" * 80)
    
    for cart in all_carts:
        user_id = cart['user_id'][:6] if len(cart['user_id']) > 8 else cart['user_id']
        
        print(f"{cart['source']:<12} "
              f"{user_id:<8} "
              f"{cart['device_type']:<10} "
              f"{cart['total_items']:<7} "
              f"${cart['total_amount']:<11.2f} "
              f"{cart['status']:<10}")
    
    # Summary
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    
    # By source
    redis_count = sum(1 for c in all_carts if c['source'] == 'Redis')
    pg_count = sum(1 for c in all_carts if c['source'] == 'PostgreSQL')
    
    print(f"\nBy Source:")
    print(f"  Redis:      {redis_count} active carts")
    print(f"  PostgreSQL: {pg_count} carts (backup/abandoned)")
    
    # By device
    devices = {}
    for cart in all_carts:
        device = cart['device_type']
        devices[device] = devices.get(device, 0) + 1
    
    print(f"\nBy Device Type:")
    for device, count in sorted(devices.items(), key=lambda x: x[1], reverse=True):
        pct = (count / len(all_carts)) * 100
        print(f"  {device.title():<10}: {count:>3} carts ({pct:>5.1f}%)")
    
    # By status
    statuses = {}
    for cart in all_carts:
        status = cart['status']
        statuses[status] = statuses.get(status, 0) + 1
    
    print(f"\nBy Status:")
    for status, count in statuses.items():
        print(f"  {status.title():<10}: {count:>3} carts")
    
    # Financial
    total_value = sum(c['total_amount'] for c in all_carts)
    total_items = sum(c['total_items'] for c in all_carts)
    
    print(f"\nFinancial:")
    print(f"  Total Value: ${total_value:,.2f}")
    print(f"  Total Items: {total_items}")
    print(f"  Avg Cart:    ${total_value / len(all_carts):.2f}")
    print(f"  Avg Items:   {total_items / len(all_carts):.2f}")

else:
    print("\nNo carts found")
    print("Run: python populate_databases.py --test")

print("\n" + "="*80)

# Clean up
cursor.close()
pg_conn.close()
redis_client.close()