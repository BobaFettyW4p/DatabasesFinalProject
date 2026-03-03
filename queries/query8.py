"""
Query 8: Retrieve User's Orders with Complete Details
"""

import psycopg2
from datetime import datetime
from timing_utils import end_query_timer, start_query_timer

# ============================================================================
# DATABASE CONNECTION
# ============================================================================

pg_conn = psycopg2.connect(
    host='localhost',
    port=5432,
    database='ecommerce',
    user='postgres',
    password='postgres'
)

print("="*80)
print("QUERY 8: User Orders - Complete Details")
print("="*80)

query_start_time = start_query_timer()

# ============================================================================
# STEP 1: Find the user
# ============================================================================

print("\nStep 1: Finding user...")

cursor = pg_conn.cursor()

cursor.execute("""
    SELECT UserID, FirstName, LastName, Email
    FROM "User"
    WHERE FirstName ILIKE '%Sarah%'
    LIMIT 1
""")

user_result = cursor.fetchone()

if not user_result:
    cursor.execute('SELECT UserID, FirstName, LastName, Email FROM "User" LIMIT 1')
    user_result = cursor.fetchone()

user_id, first_name, last_name, email = user_result

print(f"   User: {first_name} {last_name} (ID: {user_id})")

# ============================================================================
# STEP 2: Query for user's orders with all details
# ============================================================================

print("\nStep 2: Querying PostgreSQL for orders...")

print("\nSQL Query:")
print("-" * 80)

sql_query = """
SELECT 
    o.OrderID,
    o.OrderNumber,
    o.Status,
    o.SubtotalAmount,
    o.TaxAmount,
    o.ShippingAmount,
    o.TotalAmount,
    o.OrderDate,
    o.ActualShipDate,
    o.ActualDeliveryDate,
    o.OrderNotes,
    
    -- Shipping Address
    sa.AddressLine1 AS ShipAddr1,
    sa.AddressLine2 AS ShipAddr2,
    sa.City AS ShipCity,
    sa.StateProvince AS ShipState,
    sa.PostalCode AS ShipZip,
    sa.Country AS ShipCountry,
    
    -- Shipping Option
    so.Name AS ShippingOptionName,
    so.EstimatedDays,
    so.Rate AS ShippingRate,
    
    -- Payment Method
    pm.AccountNumberLast4,
    pm.CardType,
    pt.TypeName AS PaymentType
    
FROM "Order" o
LEFT JOIN Address sa ON o.ShippingAddressID = sa.AddressID
LEFT JOIN ShippingOption so ON o.ShippingOptionID = so.ShippingOptionID
LEFT JOIN PaymentMethod pm ON o.PaymentMethodID = pm.PaymentMethodID
LEFT JOIN PaymentType pt ON pm.PaymentTypeID = pt.PaymentTypeID
WHERE o.UserID = %s
ORDER BY o.OrderDate DESC;
"""

cursor.execute(sql_query, (user_id,))
orders = cursor.fetchall()

print(f"\nPostgreSQL Result: {len(orders)} orders found")

# ============================================================================
# STEP 3: For each order, get order items
# ============================================================================

print("\nStep 3: Fetching order items for each order...")

order_details = []

for order_row in orders:
    (order_id, order_num, status, subtotal, tax, shipping, total, order_date,
     ship_date, delivery_date, notes,
     ship_addr1, ship_addr2, ship_city, ship_state, ship_zip, ship_country,
     ship_option, ship_days, ship_rate,
     payment_last4, card_type, payment_type) = order_row
    
    # Get order items
    cursor.execute("""
        SELECT 
            oi.OrderItemID,
            oi.ItemNameSnapshot,
            oi.Quantity,
            oi.UnitPrice,
            oi.LineTotal,
            oi.VariationDescription,
            i.ItemID
        FROM OrderItem oi
        LEFT JOIN Item i ON oi.ItemID = i.ItemID
        WHERE oi.OrderID = %s
        ORDER BY oi.OrderItemID
    """, (order_id,))
    
    items = cursor.fetchall()
    
    order_details.append({
        'order_id': order_id,
        'order_number': order_num,
        'status': status,
        'subtotal': float(subtotal),
        'tax': float(tax),
        'shipping_cost': float(shipping),
        'total': float(total),
        'order_date': order_date,
        'ship_date': ship_date,
        'delivery_date': delivery_date,
        'notes': notes,
        'shipping_address': {
            'line1': ship_addr1,
            'line2': ship_addr2,
            'city': ship_city,
            'state': ship_state,
            'zip': ship_zip,
            'country': ship_country
        },
        'shipping_option': {
            'name': ship_option,
            'estimated_days': ship_days,
            'rate': float(ship_rate) if ship_rate else 0.0
        },
        'payment_method': {
            'type': payment_type,
            'card_type': card_type,
            'last4': payment_last4
        },
        'items': [{
            'item_id': item[6],
            'name': item[1],
            'quantity': item[2],
            'unit_price': float(item[3]),
            'line_total': float(item[4]),
            'variation': item[5]
        } for item in items]
    })

print(f"   Retrieved items for {len(order_details)} orders")

# ============================================================================
# DISPLAY RESULTS
# ============================================================================

print("\n" + "="*80)
print(f"RESULTS: {len(order_details)} Orders for {first_name} {last_name}")
print("="*80)

if order_details:
    # Summary table
    print(f"\n{'Order #':<15} {'Date':<12} {'Items':<7} {'Total':<12} {'Status':<15}")
    print("-" * 80)
    
    for order in order_details:
        order_num = order['order_number']
        date = order['order_date'].strftime('%Y-%m-%d') if order['order_date'] else 'N/A'
        num_items = len(order['items'])
        total = order['total']
        status = order['status']
        
        print(f"{order_num:<15} {date:<12} {num_items:<7} ${total:<11.2f} {status:<15}")
    
    # Detailed view for each order
    print("\n" + "="*80)
    print("DETAILED ORDER INFORMATION")
    print("="*80)
    
    for idx, order in enumerate(order_details, 1):
        print(f"\n{'-' * 80}")
        print(f"ORDER #{idx}: {order['order_number']}")
        print(f"{'-' * 80}")
        
        print(f"\nOrder Details:")
        print(f"  Order ID: {order['order_id']}")
        print(f"  Status: {order['status']}")
        print(f"  Order Date: {order['order_date'].strftime('%Y-%m-%d %H:%M:%S') if order['order_date'] else 'N/A'}")
        
        if order['ship_date']:
            print(f"  Shipped: {order['ship_date'].strftime('%Y-%m-%d')}")
        
        if order['delivery_date']:
            print(f"  Delivered: {order['delivery_date'].strftime('%Y-%m-%d')}")
        
        if order['notes']:
            print(f"  Notes: {order['notes']}")
        
        # Items
        print(f"\nItems Ordered:")
        print(f"  {'Item':<45} {'Qty':<5} {'Price':<10} {'Total':<10}")
        print(f"  {'-' * 70}")
        
        for item in order['items']:
            name = item['name'][:42] + '...' if len(item['name']) > 45 else item['name']
            
            variation_str = f" ({item['variation']})" if item['variation'] else ""
            full_name = (name + variation_str)[:45]
            
            print(f"  {full_name:<45} {item['quantity']:<5} ${item['unit_price']:<9.2f} ${item['line_total']:<9.2f}")
        
        # Pricing breakdown
        print(f"\nPricing:")
        print(f"  Subtotal:  ${order['subtotal']:>10.2f}")
        print(f"  Tax:       ${order['tax']:>10.2f}")
        print(f"  Shipping:  ${order['shipping_cost']:>10.2f}")
        print(f"  {'-' * 25}")
        print(f"  Total:     ${order['total']:>10.2f}")
        
        # Shipping info
        print(f"\nShipping:")
        print(f"  Method: {order['shipping_option']['name']}")
        print(f"  Estimated Delivery: {order['shipping_option']['estimated_days']}")
        
        addr = order['shipping_address']
        print(f"  Address:")
        print(f"    {addr['line1']}")
        if addr['line2']:
            print(f"    {addr['line2']}")
        print(f"    {addr['city']}, {addr['state']} {addr['zip']}")
        print(f"    {addr['country']}")
        
        # Payment info
        print(f"\nPayment:")
        print(f"  Method: {order['payment_method']['type']}")
        if order['payment_method']['card_type']:
            print(f"  Card: {order['payment_method']['card_type']} ending in {order['payment_method']['last4']}")

else:
    print(f"\nNo orders found for {first_name} {last_name}")
    print("\nNote: The database needs order data to be generated.")
    print("Orders are not created by populate_databases.py")

# ============================================================================
# SUMMARY STATISTICS
# ============================================================================

if order_details:
    print("\n" + "="*80)
    print("ORDER SUMMARY")
    print("="*80)
    
    total_spent = sum(o['total'] for o in order_details)
    total_items = sum(len(o['items']) for o in order_details)
    avg_order_value = total_spent / len(order_details) if order_details else 0
    
    print(f"\nOverall Statistics:")
    print(f"  Total Orders: {len(order_details)}")
    print(f"  Total Spent: ${total_spent:,.2f}")
    print(f"  Total Items Purchased: {total_items}")
    print(f"  Average Order Value: ${avg_order_value:.2f}")
    
    # By status
    status_counts = {}
    for order in order_details:
        status = order['status']
        status_counts[status] = status_counts.get(status, 0) + 1
    
    print(f"\nOrders by Status:")
    for status, count in sorted(status_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"  {status.title():<15}: {count}")
    
    # Payment methods
    payment_types = {}
    for order in order_details:
        ptype = order['payment_method']['type']
        if ptype:
            payment_types[ptype] = payment_types.get(ptype, 0) + 1
    
    if payment_types:
        print(f"\nPayment Methods Used:")
        for ptype, count in payment_types.items():
            print(f"  {ptype:<15}: {count} orders")
    
    # Shipping methods
    shipping_methods = {}
    for order in order_details:
        ship_method = order['shipping_option']['name']
        if ship_method:
            shipping_methods[ship_method] = shipping_methods.get(ship_method, 0) + 1
    
    if shipping_methods:
        print(f"\nShipping Methods Used:")
        for method, count in shipping_methods.items():
            print(f"  {method:<20}: {count} orders")

# Clean up
cursor.close()
pg_conn.close()

print("\nQuery complete!")
end_query_timer(query_start_time, "Query 8")
