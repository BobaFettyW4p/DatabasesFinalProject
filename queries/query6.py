"""
Query 6 SIMPLE: User Search Analysis - Frequency and Time of Day
Clean version without emojis
"""

import psycopg2
import pymongo
from datetime import datetime, timedelta

# Connect
pg_conn = psycopg2.connect(
    host='localhost', port=5432, database='ecommerce',
    user='postgres', password='postgres'
)

mongo_client = pymongo.MongoClient('localhost', 27017)
mongo_db = mongo_client['ecommerce']

print("="*80)
print("QUERY 6: User Search Analysis - Frequency and Time of Day")
print("="*80)

# ============================================================================
# Get user
# ============================================================================

cursor = pg_conn.cursor()

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

print(f"\nUser: {first_name} {last_name} (ID: {user_id})")

# ============================================================================
# MongoDB Aggregation
# ============================================================================

print("\nMongoDB Aggregation Pipeline:")
print("-" * 80)

print("""
db.user_events.aggregate([
    { $match: { 
        user_id: 'user_X',
        event_type: 'search'
    }},
    { $addFields: {
        hour: { $hour: '$timestamp' },
        search_query: '$event_data.search_query',
        time_of_day: {
            $switch: {
                branches: [
                    { case: { $lt: ['$hour', 6] }, then: 'night' },
                    { case: { $lt: ['$hour', 12] }, then: 'morning' },
                    { case: { $lt: ['$hour', 18] }, then: 'afternoon' },
                    { case: { $gte: ['$hour', 18] }, then: 'evening' }
                ]
            }
        }
    }},
    { $group: {
        _id: '$search_query',
        frequency: { $sum: 1 },
        times_of_day: { $push: '$time_of_day' },
        last_search: { $max: '$timestamp' }
    }},
    { $sort: { frequency: -1 } }
])
""")

# Execute aggregation
pipeline = [
    {'$match': {
        'user_id': f'user_{user_id}',
        'event_type': 'search'
    }},
    {'$addFields': {
        'hour': {'$hour': '$timestamp'},
        'search_query': '$event_data.search_query',
        'time_of_day': {
            '$switch': {
                'branches': [
                    {'case': {'$lt': ['$hour', 6]}, 'then': 'night'},
                    {'case': {'$lt': ['$hour', 12]}, 'then': 'morning'},
                    {'case': {'$lt': ['$hour', 18]}, 'then': 'afternoon'},
                    {'case': {'$gte': ['$hour', 18]}, 'then': 'evening'}
                ]
            }
        }
    }},
    {'$group': {
        '_id': '$search_query',
        'frequency': {'$sum': 1},
        'times_of_day': {'$push': '$time_of_day'},
        'last_search': {'$max': '$timestamp'}
    }},
    {'$sort': {'frequency': -1}}
]

results = list(mongo_db.user_events.aggregate(pipeline))

print(f"MongoDB Result: {len(results)} unique search terms")

# ============================================================================
# Process Results
# ============================================================================

# Categorize by frequency
high_freq = []
medium_freq = []
low_freq = []

# Categorize by time of day
by_time = {
    'morning': [],
    'afternoon': [],
    'evening': [],
    'night': []
}

for search in results:
    query = search['_id']
    freq = search['frequency']
    times = search['times_of_day']
    last = search.get('last_search')
    
    # Count time distribution
    time_counts = {
        'morning': times.count('morning'),
        'afternoon': times.count('afternoon'),
        'evening': times.count('evening'),
        'night': times.count('night')
    }
    
    most_common_time = max(time_counts.items(), key=lambda x: x[1])[0]
    
    search_data = {
        'query': query,
        'frequency': freq,
        'time_counts': time_counts,
        'most_common_time': most_common_time,
        'last_search': last
    }
    
    # Frequency categories
    if freq >= 5:
        high_freq.append(search_data)
    elif freq >= 2:
        medium_freq.append(search_data)
    else:
        low_freq.append(search_data)
    
    # Time categories
    by_time[most_common_time].append(search_data)

# ============================================================================
# Display Results
# ============================================================================

print("\n" + "="*80)
print("SEARCH TERMS BY FREQUENCY")
print("="*80)

if high_freq:
    print("\nHIGH FREQUENCY (5+ searches):")
    print("-" * 80)
    for s in high_freq:
        print(f"\n  '{s['query']}' - {s['frequency']} searches")
        print(f"  Most common: {s['most_common_time'].title()}")
        print(f"  Distribution: Morning: {s['time_counts']['morning']}, "
              f"Afternoon: {s['time_counts']['afternoon']}, "
              f"Evening: {s['time_counts']['evening']}, "
              f"Night: {s['time_counts']['night']}")

if medium_freq:
    print("\nMEDIUM FREQUENCY (2-4 searches):")
    print("-" * 80)
    for s in medium_freq:
        print(f"  '{s['query']}' - {s['frequency']} searches ({s['most_common_time']})")

if low_freq:
    print("\nLOW FREQUENCY (1 search):")
    print("-" * 80)
    queries = [s['query'] for s in low_freq[:10]]
    print(f"  {', '.join(queries)}")
    if len(low_freq) > 10:
        print(f"  ... and {len(low_freq) - 10} more")

# ============================================================================
# Display by Time of Day
# ============================================================================

print("\n" + "="*80)
print("SEARCH PATTERNS BY TIME OF DAY")
print("="*80)

time_labels = {
    'morning': 'MORNING (6 AM - 12 PM)',
    'afternoon': 'AFTERNOON (12 PM - 6 PM)',
    'evening': 'EVENING (6 PM - 12 AM)',
    'night': 'NIGHT (12 AM - 6 AM)'
}

for period, label in time_labels.items():
    searches = by_time[period]
    if searches:
        print(f"\n{label}")
        print("-" * 80)
        for s in searches[:5]:
            print(f"  '{s['query']}' ({s['frequency']} searches)")
        if len(searches) > 5:
            print(f"  ... and {len(searches) - 5} more")

# ============================================================================
# Summary Statistics
# ============================================================================

print("\n" + "="*80)
print("SUMMARY STATISTICS")
print("="*80)

if results:
    total_searches = sum(s['frequency'] for s in results)
    
    print(f"\nTotal Searches: {total_searches}")
    print(f"Unique Terms: {len(results)}")
    print(f"Average Searches per Term: {total_searches / len(results):.2f}")
    
    # Time distribution
    all_time_counts = {'morning': 0, 'afternoon': 0, 'evening': 0, 'night': 0}
    
    # Combine all processed search data
    all_searches = high_freq + medium_freq + low_freq
    
    for s in all_searches:
        for period in ['morning', 'afternoon', 'evening', 'night']:
            all_time_counts[period] += s['time_counts'][period]
    
    print(f"\nSearches by Time of Day:")
    for period, count in sorted(all_time_counts.items(), key=lambda x: x[1], reverse=True):
        pct = (count / total_searches) * 100 if total_searches > 0 else 0
        print(f"  {period.title():<12}: {count:>3} searches ({pct:>5.1f}%)")
    
    # Peak time
    peak_period = max(all_time_counts.items(), key=lambda x: x[1])
    print(f"\nPeak Search Time: {peak_period[0].title()} ({peak_period[1]} searches)")
    
    # Frequency breakdown
    print(f"\nFrequency Breakdown:")
    print(f"  High (5+):    {len(high_freq)} terms")
    print(f"  Medium (2-4): {len(medium_freq)} terms")
    print(f"  Low (1):      {len(low_freq)} terms")
    
    # Top searches
    print(f"\nTop 5 Search Terms:")
    for idx, s in enumerate(results[:5], 1):
        print(f"  {idx}. '{s['_id']}' - {s['frequency']} searches")

else:
    print("\nNo search data found for this user")

print("\n" + "="*80)

# Clean up
cursor.close()
pg_conn.close()
mongo_client.close()