"""
Query 13: Days Since Last Purchase & Order Count
"""

import psycopg2
from datetime import datetime

# Connect
pg_conn = psycopg2.connect(
    host='localhost', port=5432, database='ecommerce',
    user='postgres', password='postgres'
)

print("="*80)
print("QUERY 13: Days Since Last Purchase & Order Count")
print("="*80)

cursor = pg_conn.cursor()

sql = """
SELECT 
    u.UserID,
    u.FirstName,
    u.LastName,
    COUNT(o.OrderID) AS TotalOrders,
    MAX(o.OrderDate) AS LastPurchaseDate,
    EXTRACT(DAY FROM CURRENT_TIMESTAMP - MAX(o.OrderDate)) AS DaysSinceLastPurchase
FROM "User" u
LEFT JOIN "Order" o ON u.UserID = o.UserID
GROUP BY u.UserID, u.FirstName, u.LastName
ORDER BY DaysSinceLastPurchase ASC NULLS LAST;
"""

cursor.execute(sql)
results = cursor.fetchall()

# Display
print("\n" + "="*80)
print(f"RESULTS: {len(results)} Users")
print("="*80)

print(f"\n{'User':<25} {'Total Orders':<15} {'Days Since Last Purchase':<25}")
print("-" * 80)

for user_id, first_name, last_name, total_orders, last_purchase, days_since in results:
    name = f"{first_name} {last_name}"
    
    if last_purchase:
        days_str = f"{int(days_since)} days ago"
    else:
        days_str = "Never purchased"
    
    print(f"{name:<25} {total_orders:<15} {days_str:<25}")

print("\n" + "="*80)

# Clean up
cursor.close()
pg_conn.close()