"""
Query 10: Average Days Between Purchases
"""

import psycopg2

# Connect
pg_conn = psycopg2.connect(
    host='localhost', port=5432, database='ecommerce',
    user='postgres', password='postgres'
)

print("="*80)
print("QUERY 10: Average Days Between Purchases")
print("="*80)

cursor = pg_conn.cursor()

# Get user
cursor.execute("""
    SELECT UserID, FirstName, LastName
    FROM "User"
    WHERE FirstName ILIKE '%Sarah%'
    LIMIT 1
""")

user_result = cursor.fetchone()

if not user_result:
    cursor.execute('SELECT UserID, FirstName, LastName FROM "User" LIMIT 1')
    user_result = cursor.fetchone()

user_id, first_name, last_name = user_result

print(f"\nUser: {first_name} {last_name}")

# Query
print("\nSQL Query:")
print("-" * 80)

sql = """
WITH OrderIntervals AS (
    SELECT 
        OrderDate,
        LAG(OrderDate) OVER (ORDER BY OrderDate) AS PreviousOrderDate
    FROM "Order"
    WHERE UserID = %s
)
SELECT 
    AVG(EXTRACT(DAY FROM OrderDate - PreviousOrderDate)) AS AvgDaysBetweenPurchases
FROM OrderIntervals
WHERE PreviousOrderDate IS NOT NULL;
"""

print(sql)

cursor.execute(sql, (user_id,))
result = cursor.fetchone()

# Display result
print("\n" + "="*80)
print("RESULT")
print("="*80)

if result and result[0] is not None:
    avg_days = float(result[0])
    print(f"\nAverage Days Between Purchases: {avg_days:.1f} days")
else:
    print("\nNot enough orders to calculate average (need at least 2 orders)")

print("\n" + "="*80)

# Clean up
cursor.close()
pg_conn.close()