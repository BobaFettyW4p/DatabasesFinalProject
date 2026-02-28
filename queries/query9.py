"""
Query 9: User's Returned Items - Refund Status, Amount, Restocking Fees
Shows all returns for a user with complete details
Pure PostgreSQL query with JOINs across Order, OrderItem, and ReturnItem tables
"""

import psycopg2
from datetime import datetime

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
print("QUERY 9: User's Returned Items - Refund Details")
print("="*80)

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
# STEP 2: Query for user's returned items
# ============================================================================

print("\nStep 2: Querying for returned items...")

print("\nSQL Query:")
print("-" * 80)

sql_query = """
SELECT 
    ri.ReturnID,
    ri.Status AS ReturnStatus,
    ri.Quantity AS ReturnedQuantity,
    ri.RefundAmount,
    ri.RestockingFee,
    ri.RefundStatus,
    ri.ReturnReason,
    ri.ReturnNotes,
    ri.RequestedAt,
    ri.ApprovedAt,
    ri.ReceivedAt,
    ri.RefundedAt,
    
    -- Order Info
    o.OrderID,
    o.OrderNumber,
    o.OrderDate,
    
    -- Item Info
    i.ItemID,
    i.Name AS ItemName,
    oi.UnitPrice AS OriginalPrice,
    oi.Quantity AS OrderedQuantity,
    oi.LineTotal AS OriginalLineTotal
    
FROM ReturnItem ri
JOIN "Order" o ON ri.OrderID = o.OrderID
JOIN OrderItem oi ON ri.OrderItemID = oi.OrderItemID
JOIN Item i ON ri.ItemID = i.ItemID
WHERE o.UserID = %s
ORDER BY ri.RequestedAt DESC;
"""

print(sql_query)

cursor.execute(sql_query, (user_id,))
returns = cursor.fetchall()

print(f"\nPostgreSQL Result: {len(returns)} returned items found")

# ============================================================================
# PROCESS RESULTS
# ============================================================================

return_details = []

for row in returns:
    (return_id, return_status, returned_qty, refund_amt, restocking_fee,
     refund_status, return_reason, return_notes, requested_at, approved_at,
     received_at, refunded_at, order_id, order_number, order_date,
     item_id, item_name, original_price, ordered_qty, original_line_total) = row
    
    return_details.append({
        'return_id': return_id,
        'return_status': return_status,
        'refund_status': refund_status,
        'returned_qty': returned_qty,
        'refund_amount': float(refund_amt),
        'restocking_fee': float(restocking_fee),
        'net_refund': float(refund_amt) - float(restocking_fee),
        'return_reason': return_reason,
        'return_notes': return_notes,
        'requested_at': requested_at,
        'approved_at': approved_at,
        'received_at': received_at,
        'refunded_at': refunded_at,
        'order_id': order_id,
        'order_number': order_number,
        'order_date': order_date,
        'item_id': item_id,
        'item_name': item_name,
        'original_price': float(original_price),
        'ordered_qty': ordered_qty,
        'original_line_total': float(original_line_total)
    })

# ============================================================================
# DISPLAY RESULTS
# ============================================================================

print("\n" + "="*80)
print(f"RESULTS: {len(return_details)} Returned Items for {first_name} {last_name}")
print("="*80)

if return_details:
    # Summary table
    print(f"\n{'Return ID':<10} {'Item':<35} {'Qty':<5} {'Refund':<10} {'Fee':<10} {'Status':<12}")
    print("-" * 80)
    
    for ret in return_details:
        item_name = ret['item_name'][:32] + '...' if len(ret['item_name']) > 35 else ret['item_name']
        
        print(f"{ret['return_id']:<10} "
              f"{item_name:<35} "
              f"{ret['returned_qty']:<5} "
              f"${ret['refund_amount']:<9.2f} "
              f"${ret['restocking_fee']:<9.2f} "
              f"{ret['refund_status']:<12}")
    
    # Detailed view
    print("\n" + "="*80)
    print("DETAILED RETURN INFORMATION")
    print("="*80)
    
    for idx, ret in enumerate(return_details, 1):
        print(f"\n{'-' * 80}")
        print(f"RETURN #{idx}: Return ID {ret['return_id']}")
        print(f"{'-' * 80}")
        
        print(f"\nItem Details:")
        print(f"  Item: {ret['item_name']}")
        print(f"  Item ID: {ret['item_id']}")
        print(f"  Original Order: {ret['order_number']} (Order #{ret['order_id']})")
        print(f"  Order Date: {ret['order_date'].strftime('%Y-%m-%d')}")
        
        print(f"\nReturn Details:")
        print(f"  Quantity Returned: {ret['returned_qty']} of {ret['ordered_qty']} ordered")
        print(f"  Return Status: {ret['return_status'].title()}")
        print(f"  Return Reason: {ret['return_reason'] or 'Not specified'}")
        if ret['return_notes']:
            print(f"  Notes: {ret['return_notes']}")
        
        print(f"\nRefund Information:")
        print(f"  Original Item Price: ${ret['original_price']:.2f} each")
        print(f"  Original Line Total: ${ret['original_line_total']:.2f}")
        print(f"  Refund Amount: ${ret['refund_amount']:.2f}")
        print(f"  Restocking Fee: ${ret['restocking_fee']:.2f}")
        print(f"  {'-' * 40}")
        print(f"  Net Refund: ${ret['net_refund']:.2f}")
        print(f"  Refund Status: {ret['refund_status'].title()}")
        
        print(f"\nTimeline:")
        print(f"  Requested: {ret['requested_at'].strftime('%Y-%m-%d %H:%M:%S')}")
        
        if ret['approved_at']:
            print(f"  Approved: {ret['approved_at'].strftime('%Y-%m-%d %H:%M:%S')}")
        
        if ret['received_at']:
            print(f"  Received: {ret['received_at'].strftime('%Y-%m-%d %H:%M:%S')}")
        
        if ret['refunded_at']:
            print(f"  Refunded: {ret['refunded_at'].strftime('%Y-%m-%d %H:%M:%S')}")

else:
    print(f"\nNo returned items found for {first_name} {last_name}")
    print("\nNote: Return data needs to be generated.")
    print("This is sample data - returns would be created when customers return items.")

# ============================================================================
# SUMMARY STATISTICS
# ============================================================================

if return_details:
    print("\n" + "="*80)
    print("RETURN SUMMARY")
    print("="*80)
    
    total_returns = len(return_details)
    total_items_returned = sum(r['returned_qty'] for r in return_details)
    total_refund_amount = sum(r['refund_amount'] for r in return_details)
    total_restocking_fees = sum(r['restocking_fee'] for r in return_details)
    net_refund_total = total_refund_amount - total_restocking_fees
    
    print(f"\nOverall Statistics:")
    print(f"  Total Returns: {total_returns}")
    print(f"  Total Items Returned: {total_items_returned}")
    print(f"  Total Refund Amount: ${total_refund_amount:,.2f}")
    print(f"  Total Restocking Fees: ${total_restocking_fees:,.2f}")
    print(f"  Net Refund: ${net_refund_total:,.2f}")
    
    # By return status
    status_counts = {}
    for ret in return_details:
        status = ret['return_status']
        status_counts[status] = status_counts.get(status, 0) + 1
    
    print(f"\nReturns by Status:")
    for status, count in sorted(status_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"  {status.title():<15}: {count}")
    
    # By refund status
    refund_status_counts = {}
    refund_by_status = {}
    for ret in return_details:
        ref_status = ret['refund_status']
        refund_status_counts[ref_status] = refund_status_counts.get(ref_status, 0) + 1
        refund_by_status[ref_status] = refund_by_status.get(ref_status, 0) + ret['refund_amount']
    
    print(f"\nRefunds by Status:")
    for ref_status, count in sorted(refund_status_counts.items(), key=lambda x: x[1], reverse=True):
        amount = refund_by_status[ref_status]
        print(f"  {ref_status.title():<15}: {count} returns (${amount:,.2f})")
    
    # Return reasons
    reason_counts = {}
    for ret in return_details:
        reason = ret['return_reason'] or 'Not specified'
        reason_counts[reason] = reason_counts.get(reason, 0) + 1
    
    print(f"\nReturn Reasons:")
    for reason, count in sorted(reason_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"  {reason:<25}: {count}")
    
    # Restocking fees analysis
    returns_with_fees = [r for r in return_details if r['restocking_fee'] > 0]
    if returns_with_fees:
        print(f"\nRestocking Fees:")
        print(f"  Returns with fees: {len(returns_with_fees)} of {total_returns}")
        print(f"  Average fee: ${sum(r['restocking_fee'] for r in returns_with_fees) / len(returns_with_fees):.2f}")

# Clean up
cursor.close()
pg_conn.close()

print("\nQuery complete!")