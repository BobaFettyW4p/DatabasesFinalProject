"""
Query 1: Retrieve all products in "fashion" category with attributes
Combines PostgreSQL (items/categories) with MongoDB (flexible attributes)
"""

import psycopg2
import pymongo
from pprint import pprint

# Database connections
pg_conn = psycopg2.connect(
    host='localhost',
    port=5432,
    database='ecommerce',
    user='postgres',
    password='postgres'
)

mongo_client = pymongo.MongoClient('localhost', 27017)
mongo_db = mongo_client['ecommerce']

print("="*80)
print("QUERY 1: Fashion Products with Attributes")
print("="*80)

# Step 1: Get fashion products from PostgreSQL
print("\nStep 1: Querying PostgreSQL for fashion category items...")

pg_cursor = pg_conn.cursor()

query = """
    SELECT 
        i.ItemID,
        i.Name,
        i.Description,
        i.BasePrice,
        i.TotalStockQuantity,
        i.ImageURL,
        c.CategoryName
    FROM Item i
    JOIN ItemCategory ic ON i.ItemID = ic.ItemID
    JOIN Category c ON ic.CategoryID = c.CategoryID
    WHERE c.CategoryName = 'Fashion'
      AND i.IsActive = true
    ORDER BY i.Name
    LIMIT 20;
"""

pg_cursor.execute(query)
products = pg_cursor.fetchall()

print(f"Found {len(products)} fashion products in PostgreSQL")

# Step 2: For each product, get attributes from MongoDB
print("\nStep 2: Enriching with MongoDB attributes...\n")

results = []

for product in products:
    item_id, name, description, base_price, stock, image_url, category = product
    
    # Query MongoDB for this product's attributes
    mongo_doc = mongo_db.product_attributes.find_one({
        'product_id': f'item_{item_id}'
    })
    
    # Combine PostgreSQL + MongoDB data
    result = {
        'item_id': item_id,
        'name': name,
        'description': description,
        'base_price': float(base_price),
        'stock': stock,
        'category': category,
        'image_url': image_url
    }
    
    # Add MongoDB attributes if found
    if mongo_doc:
        result['material'] = mongo_doc.get('attributes', {}).get('material', 'N/A')
        result['available_sizes'] = mongo_doc.get('attributes', {}).get('available_sizes', [])
        result['available_colors'] = mongo_doc.get('attributes', {}).get('available_colors', [])
        result['pattern'] = mongo_doc.get('attributes', {}).get('pattern', 'N/A')
        result['stock_by_variant'] = mongo_doc.get('stock_by_variant', {})
    else:
        result['material'] = 'N/A'
        result['available_sizes'] = []
        result['available_colors'] = []
        result['pattern'] = 'N/A'
        result['stock_by_variant'] = {}
    
    results.append(result)

# Display results
print("="*80)
print(f"RESULTS: {len(results)} Fashion Products")
print("="*80)

for idx, product in enumerate(results, 1):
    print(f"\n{idx}. {product['name']}")
    print(f"   Price: ${product['base_price']:.2f}")
    print(f"   Total Stock: {product['stock']}")
    print(f"   Material: {product['material']}")
    print(f"   Available Sizes: {', '.join(product['available_sizes']) if product['available_sizes'] else 'N/A'}")
    print(f"   Available Colors: {', '.join(product['available_colors']) if product['available_colors'] else 'N/A'}")
    print(f"   Pattern: {product['pattern']}")
    
    if product['stock_by_variant']:
        print(f"   Stock by Variant:")
        for variant, qty in product['stock_by_variant'].items():
            print(f"      • {variant}: {qty} units")

print("\n" + "="*80)

# Summary statistics
total_stock = sum(p['stock'] for p in results)
avg_price = sum(p['base_price'] for p in results) / len(results) if results else 0
all_materials = set(p['material'] for p in results if p['material'] != 'N/A')
all_sizes = set()
all_colors = set()

for p in results:
    all_sizes.update(p['available_sizes'])
    all_colors.update(p['available_colors'])

print("\nSUMMARY STATISTICS:")
print(f"  Total Products: {len(results)}")
print(f"  Total Stock: {total_stock} units")
print(f"  Average Price: ${avg_price:.2f}")
print(f"  Unique Materials: {', '.join(all_materials) if all_materials else 'None'}")
print(f"  All Available Sizes: {', '.join(sorted(all_sizes)) if all_sizes else 'None'}")
print(f"  All Available Colors: {', '.join(sorted(all_colors)) if all_colors else 'None'}")
print("="*80)

# Clean up
pg_cursor.close()
pg_conn.close()
mongo_client.close()

print("\nQuery complete!")