# Database Setup Instructions

This guide walks through setting up all 4 databases and populating them with synthetic data.

## Prerequisites

1. **Python 3.8+** installed
2. **Docker** (recommended) or local installations of:
   - PostgreSQL 14+
   - MongoDB 6+
   - Redis 7+
   - Neo4j 5+

---

## Quick Start with Docker

### 1. Start All Databases

```bash
# PostgreSQL
docker run --name ecommerce-postgres\
  -e POSTGRES_PASSWORD=postgres\
  -e POSTGRES_DB=ecommerce \
  -p 5432:5432\
  -d postgres:14

# MongoDB
docker run --name ecommerce-mongo \
  -p 27017:27017 \
  -d mongo:6

# Redis
docker run --name ecommerce-redis \
  -p 6379:6379 \
  -d redis:7

# Neo4j
docker run --name ecommerce-neo4j \
  -p 7474:7474 -p 7687:7687 \
  -e NEO4J_AUTH=neo4j/your_password \
  -d neo4j:5
```

### 2. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 3. Stop Local MongoDB (If Running)

**IMPORTANT:** If you have MongoDB installed locally via Homebrew, it will conflict with Docker MongoDB on port 27017.

```bash
# Check if local MongoDB is running
lsof -i :27017

# If you see "mongod" (not Docker), stop it:
brew services stop mongodb-community

# Verify only Docker MongoDB is running
lsof -i :27017
# Should show: com.docker (Docker process only)
```

### 4. Update Database Configuration

Edit `populate_databases.py` and update the `DB_CONFIG` section:

```python
DB_CONFIG = {
    'postgresql': {
        'host': 'localhost',
        'port': 5432,
        'database': 'ecommerce',
        'user': 'postgres',
        'password': 'postgres'  # Change if needed
    },
    'mongodb': {
        'host': 'localhost',
        'port': 27017,
        'database': 'ecommerce'
    },
    'redis': {
        'host': 'localhost',
        'port': 6379,
        'db': 0
    },
    'neo4j': {
        'uri': 'bolt://localhost:7687',
        'user': 'neo4j',
        'password': 'your_password'  # MUST match Docker password!
    }
}
```

### 5. Run the Population Script

```bash
# Test mode (10 users, 20 products)
python populate_databases.py --test
```

The script will automatically:

- Create all PostgreSQL tables (17 tables)
- Clear existing data in all databases
- Insert synthetic data into all 4 databases
- Create indexes and constraints
- Properly link products to matching categories

## Clean Slate (Start Over)

To completely wipe all data and start fresh:

```bash
# PostgreSQL
docker exec -it ecommerce-postgres psql -U postgres -d ecommerce \
  -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"

# MongoDB
docker exec -it ecommerce-mongo mongosh --eval "use ecommerce; db.dropDatabase()"

# Redis
docker exec -it ecommerce-redis redis-cli FLUSHDB

# Neo4j (via browser)
# Go to http://localhost:7474 and run:
MATCH (n) DETACH DELETE n

# Then re-populate
python populate_databases.py --test
```

---

## Full Dataset

For the complete dataset (1000 users, 5000 products, 100K orders):

```bash
python populate_databases.py --full
```

**Warning:** This takes 10-30 minutes depending on hardware.

---

## Stop Containers (Keep Data)

```bash
docker stop ecommerce-postgres ecommerce-mongo ecommerce-redis ecommerce-neo4j
```

## Restart Containers

```bash
docker start ecommerce-postgres ecommerce-mongo ecommerce-redis ecommerce-neo4j
```

## Remove Containers (Delete Everything)

```bash
docker stop ecommerce-postgres ecommerce-mongo ecommerce-redis ecommerce-neo4j
docker rm ecommerce-postgres ecommerce-mongo ecommerce-redis ecommerce-neo4j
```
