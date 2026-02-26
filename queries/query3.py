"""
Query 3: Low Stock Items (< 5 units)
Straightforward version showing just the query and results
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
print("QUERY 3: Items with Stock < 5 Units")
print("="*80)

# ============================================================================
# POSTGRESQL QUERY
# ============================================================================

print("\nPostgreSQL Query:")
print("-" * 80)

sql = """
SELECT 
    i.ItemID,
    i.Name,
    i.BasePrice,
    i.TotalStockQuantity,
    c.CategoryName
FROM Item i
LEFT JOIN ItemCategory ic ON i.ItemID = ic.ItemID
LEFT JOIN Category c ON ic.CategoryID = c.CategoryID
WHERE i.TotalStockQuantity < 5
  AND i.IsActive = true
ORDER BY i.TotalStockQuantity ASC, i.Name;
"""

print(sql)

cursor = pg_conn.cursor()
cursor.execute(sql)
results = cursor.fetchall()

print(f"\nResults: {len(results)} items with low stock\n")

# ============================================================================
# DISPLAY RESULTS
# ============================================================================

if results:
    print("Low Stock Items:")
    print("-" * 80)
    print(f"{'ID':<5} {'Name':<40} {'Category':<20} {'Stock':<8} {'Price':<10}")
    print("-" * 80)
    
    for row in results:
        item_id, name, price, stock, category = row
        category = category if category else 'N/A'
        name_short = name[:37] + '...' if len(name) > 40 else name
        print(f"{item_id:<5} {name_short:<40} {category:<20} {stock:<8} ${price:<9.2f}")
    
    # ========================================================================
    # GET VARIANT DETAILS FROM MONGODB
    # ========================================================================
    
    print("\n" + "="*80)
    print("Variant-Level Stock Details (from MongoDB)")
    print("="*80)
    
    for row in results[:5]:  # Show details for first 5 items
        item_id, name, price, stock, category = row
        
        # Get MongoDB details
        mongo_doc = mongo_db.product_attributes.find_one({
            'product_id': f'item_{item_id}'
        })
        
        print(f"\n{name} (Total: {stock} units)")
        
        if mongo_doc and 'stock_by_variant' in mongo_doc:
            stock_by_variant = mongo_doc['stock_by_variant']
            print("  Variants:")
            for variant, qty in stock_by_variant.items():
                status = "OUT" if qty == 0 else "🟠 CRITICAL" if qty <= 2 else "🟡 LOW"
                print(f"    - {variant:<20}: {qty:>2} units {status if qty < 5 else ''}")
        else:
            print("  No variant details available")
    
    if len(results) > 5:
        print(f"\n  ... and {len(results) - 5} more items")
    
    # ========================================================================
    # SUMMARY
    # ========================================================================
    
    print("\n" + "="*80)
    print("Summary")
    print("="*80)
    
    out_of_stock = sum(1 for r in results if r[3] == 0)
    critical = sum(1 for r in results if 0 < r[3] <= 2)
    low = sum(1 for r in results if 3 <= r[3] < 5)
    
    print(f"\nTotal Items Needing Attention: {len(results)}")
    print(f"  - Out of Stock (0): {out_of_stock}")
    print(f"  - Critical (1-2): {critical}")
    print(f"  - Low (3-4): {low}")
    
    total_value = sum(float(r[2]) * r[3] for r in results)
    print(f"\nTotal Stock Value: ${total_value:,.2f}")
    
else:
    print("All items are well-stocked! (Stock ≥ 5 units)")

print("\n" + "="*80)

# Clean up
cursor.close()
pg_conn.close()
mongo_client.close()