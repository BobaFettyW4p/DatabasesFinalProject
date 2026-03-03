"""
Query 12: Products Purchased Together with Headphones
"""

import psycopg2
from timing_utils import end_query_timer, start_query_timer

# Connect
pg_conn = psycopg2.connect(
    host='localhost', port=5432, database='ecommerce',
    user='postgres', password='postgres'
)

print("="*80)
print("QUERY 12: Products Purchased Together with Headphones")
print("="*80)

query_start_time = start_query_timer()

cursor = pg_conn.cursor()

# Find headphones
cursor.execute("""
    SELECT ItemID, Name
    FROM Item
    WHERE Name ILIKE '%headphone%'
      AND IsActive = true
""")

headphone_products = cursor.fetchall()

if not headphone_products:
    print("\nNo headphones found in database")
    cursor.close()
    pg_conn.close()
    end_query_timer(query_start_time, "Query 12")
    raise SystemExit(0)

print(f"\nHeadphone Product: {headphone_products[0][1]}")

headphone_ids = [item[0] for item in headphone_products]

# Find products purchased together
print("\nSQL Query:")
print("-" * 80)

sql = """
WITH HeadphoneOrders AS (
    SELECT DISTINCT OrderID
    FROM OrderItem
    WHERE ItemID IN %s
)
SELECT 
    i.Name,
    COUNT(*) AS TimesTogetherCount
FROM OrderItem oi
JOIN Item i ON oi.ItemID = i.ItemID
JOIN HeadphoneOrders ho ON oi.OrderID = ho.OrderID
WHERE oi.ItemID NOT IN %s
GROUP BY i.ItemID, i.Name
ORDER BY COUNT(*) DESC
LIMIT 3;
"""

print(sql)

cursor.execute(sql, (tuple(headphone_ids), tuple(headphone_ids)))
results = cursor.fetchall()

# Display results
print("\n" + "="*80)
print("RESULTS: Top 3 Products")
print("="*80)

if results:
    print(f"\n{'Rank':<6} {'Product':<50} {'Times Together':<15}")
    print("-" * 80)
    
    for idx, (name, count) in enumerate(results, 1):
        rank = {1: "1st", 2: "2nd", 3: "3rd"}.get(idx, f"{idx}th")
        print(f"{rank:<6} {name:<50} {count} times")
else:
    print("\nNo products found purchased together with headphones")

print("\n" + "="*80)

# Clean up
cursor.close()
pg_conn.close()
end_query_timer(query_start_time, "Query 12")
