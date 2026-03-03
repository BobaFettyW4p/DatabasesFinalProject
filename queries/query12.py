"""
Query 12: Products Purchased Together with Headphones
Neo4j implementation using PURCHASED_WITH relationships
"""

import psycopg2
from neo4j import GraphDatabase
from timing_utils import end_query_timer, start_query_timer

# Connect
pg_conn = psycopg2.connect(
    host='localhost', port=5432, database='ecommerce',
    user='postgres', password='postgres'
)

neo4j_driver = GraphDatabase.driver(
    'bolt://localhost:7687',
    auth=('neo4j', 'your_password')
)

print("="*80)
print("QUERY 12: Products Purchased Together with Headphones")
print("="*80)

query_start_time = start_query_timer()

# ============================================================================
# NEO4J GRAPH TRAVERSAL
# ============================================================================

print("\nNeo4j Cypher Query:")
print("-" * 80)

cypher_query = """
MATCH (headphones:Product)-[pw:PURCHASED_WITH]-(other:Product)
WHERE headphones.name CONTAINS 'Headphone'
RETURN other.name as product_name,
       pw.count as times_together,
       pw.confidence as confidence_score
ORDER BY pw.count DESC
LIMIT 3
"""

print(cypher_query)

final_results = []

try:
    with neo4j_driver.session() as session:
        result = session.run(cypher_query)
        
        for record in result:
            final_results.append({
                'name': record['product_name'],
                'count': record['times_together'],
                'confidence': record['confidence_score']
            })
    
    if final_results:
        print(f"\nNeo4j Result: Found {len(final_results)} products purchased together")
    else:
        print(f"\nNeo4j Result: No co-purchase patterns found")

except Exception as e:
    print(f"\n⚠ Neo4j query failed: {e}")
    print("\nFalling back to PostgreSQL approach...")
    
    # Fallback to PostgreSQL
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
    print("\nPostgreSQL Query:")
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
    pg_results = cursor.fetchall()
    
    for name, count in pg_results:
        final_results.append({
            'name': name,
            'count': count,
            'confidence': None
        })
    
    cursor.close()

# ============================================================================
# DISPLAY RESULTS
# ============================================================================

print("\n" + "="*80)
print("RESULTS: Top 3 Products Purchased with Headphones")
print("="*80)

if final_results:
    # Check if we have confidence scores (Neo4j) or not (PostgreSQL)
    has_confidence = final_results[0].get('confidence') is not None
    
    if has_confidence:
        print(f"\n{'Rank':<6} {'Product':<45} {'Times':<10} {'Confidence':<12}")
        print("-" * 80)
        
        for idx, item in enumerate(final_results, 1):
            rank = {1: "1st", 2: "2nd", 3: "3rd"}.get(idx, f"{idx}th")
            name_short = item['name'][:42] + '...' if len(item['name']) > 45 else item['name']
            confidence_pct = f"{item['confidence']*100:.1f}%" if item['confidence'] else "N/A"
            print(f"{rank:<6} {name_short:<45} {item['count']:<10} {confidence_pct:<12}")
    else:
        print(f"\n{'Rank':<6} {'Product':<50} {'Times Together':<15}")
        print("-" * 80)
        
        for idx, item in enumerate(final_results, 1):
            rank = {1: "1st", 2: "2nd", 3: "3rd"}.get(idx, f"{idx}th")
            print(f"{rank:<6} {item['name']:<50} {item['count']} times")
    
    # Summary
    print("\n" + "="*80)
    print("Analysis")
    print("="*80)
    
    total_pairs = sum(item['count'] for item in final_results)
    print(f"\nTotal co-purchases analyzed: {total_pairs:,}")
    
    print(f"\nThese products are frequently bought together with headphones,")
    print(f"suggesting strong complementary relationships. Consider:")
    print(f"  - Product bundling with 10% discount")
    print(f"  - Cross-sell recommendations on product page")
    print(f"  - Targeted email campaigns")
    
    if has_confidence:
        print(f"\nConfidence Score: Indicates strength of co-purchase pattern")
        print(f"  - 0-30%:  Weak association")
        print(f"  - 30-60%: Moderate association")
        print(f"  - 60%+:   Strong association")

else:
    print("\nNo products found purchased together with headphones")
    print("This could indicate:")
    print("  - Headphones are typically purchased alone")
    print("  - Insufficient order data")
    print("  - Need to populate database: python populate_databases.py --full")

print("\n" + "="*80)

# Clean up
pg_conn.close()
neo4j_driver.close()
end_query_timer(query_start_time, "Query 12")