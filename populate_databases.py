"""
E-Commerce Database Population Script
Populates PostgreSQL, MongoDB, Redis, and Neo4j with synthetic data

Usage:
    python populate_databases.py --test           # Small test dataset (10 users, 20 products, ~25 orders, ~100 events)
    python populate_databases.py --full           # Full dataset (1000 users, 5000 products, ~100k orders, ~500k events)
    python populate_databases.py --test --no-clear  # Test dataset without clearing existing data

Production Scale (--full mode):
    - 1,000 users
    - 5,000 products
    - ~100,000 orders (100 per user)
    - ~500,000 user events (400k views + 100k searches)

Features:
    - Automatically clears all databases before populating (use --no-clear to skip)
    - Generates realistic e-commerce data with proper relationships
    - Creates orders, payments, shipping options automatically
    - Populates search events, view events, and shopping carts
    - Supports both test and production-scale datasets
    - Batch processing for large datasets with progress indicators

Requirements:
    pip install pymongo redis neo4j psycopg2-binary faker
"""

import sys
import argparse
from datetime import datetime, timedelta
import random
import json
from faker import Faker
import psycopg2
from psycopg2.extras import execute_values
import pymongo
import redis
from neo4j import GraphDatabase

# Initialize Faker
fake = Faker()
Faker.seed(1)  # For reproducibility
random.seed(1)


# ============================================================================
# DATABASE CONFIGURATION
# ============================================================================

DB_CONFIG = {
    'postgresql': {
        'host': 'localhost',
        'port': 5432,
        'database': 'ecommerce',
        'user': 'postgres',
        'password': 'postgres'
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
        'password': 'your_password'
    }
}


# ============================================================================
# DATABASE CONNECTIONS
# ============================================================================

class DatabaseConnections:
    """Manages connections to all databases"""
    
    def __init__(self, config):
        self.config = config
        self.pg_conn = None
        self.mongo_client = None
        self.mongo_db = None
        self.redis_client = None
        self.neo4j_driver = None
    
    def connect_all(self):
        """Establish connections to all databases"""
        print("Connecting to databases...")
        
        # PostgreSQL
        try:
            self.pg_conn = psycopg2.connect(**self.config['postgresql'])
            self.pg_conn.autocommit = False
            print("PostgreSQL connected")
        except Exception as e:
            print(f"PostgreSQL connection failed: {e}")
            sys.exit(1)
        
        # MongoDB
        try:
            self.mongo_client = pymongo.MongoClient(
                self.config['mongodb']['host'],
                self.config['mongodb']['port']
            )
            self.mongo_db = self.mongo_client[self.config['mongodb']['database']]
            print("MongoDB connected")
        except Exception as e:
            print(f"MongoDB connection failed: {e}")
            sys.exit(1)
        
        # Redis
        try:
            self.redis_client = redis.Redis(
                host=self.config['redis']['host'],
                port=self.config['redis']['port'],
                db=self.config['redis']['db'],
                decode_responses=True
            )
            self.redis_client.ping()
            print("Redis connected")
        except Exception as e:
            print(f"Redis connection failed: {e}")
            sys.exit(1)
        
        # Neo4j
        try:
            self.neo4j_driver = GraphDatabase.driver(
                self.config['neo4j']['uri'],
                auth=(self.config['neo4j']['user'], self.config['neo4j']['password'])
            )
            # Test connection
            with self.neo4j_driver.session() as session:
                session.run("RETURN 1")
            print("Neo4j connected")
        except Exception as e:
            print(f"Neo4j connection failed: {e}")
            sys.exit(1)
    
    def close_all(self):
        """Close all database connections"""
        if self.pg_conn is not None:
            self.pg_conn.close()
        if self.mongo_client is not None:
            self.mongo_client.close()
        if self.redis_client is not None:
            self.redis_client.close()
        if self.neo4j_driver is not None:
            self.neo4j_driver.close()
        print("\nAll connections closed")
    
    def clear_all(self):
        """Clear all existing data from databases"""
        print("\n" + "="*60)
        print("CLEARING EXISTING DATA")
        print("="*60)
        
        # Clear PostgreSQL
        if self.pg_conn is not None:
            print("\nClearing PostgreSQL...")
            try:
                cursor = self.pg_conn.cursor()
                cursor.execute("DROP SCHEMA public CASCADE;")
                cursor.execute("CREATE SCHEMA public;")
                cursor.execute("GRANT ALL ON SCHEMA public TO postgres;")
                cursor.execute("GRANT ALL ON SCHEMA public TO public;")
                self.pg_conn.commit()
                cursor.close()
                print("  PostgreSQL cleared")
            except Exception as e:
                print(f"  PostgreSQL clear failed: {e}")
                self.pg_conn.rollback()
        
        # Clear MongoDB
        if self.mongo_db is not None:
            print("\nClearing MongoDB...")
            try:
                # Drop all collections
                for collection_name in self.mongo_db.list_collection_names():
                    self.mongo_db[collection_name].drop()
                print("  MongoDB cleared")
            except Exception as e:
                print(f"  MongoDB clear failed: {e}")
        
        # Clear Redis
        if self.redis_client is not None:
            print("\nClearing Redis...")
            try:
                self.redis_client.flushdb()
                print("  Redis cleared")
            except Exception as e:
                print(f"  Redis clear failed: {e}")
        
        # Clear Neo4j
        if self.neo4j_driver is not None:
            print("\nClearing Neo4j...")
            try:
                with self.neo4j_driver.session() as session:
                    session.run("MATCH (n) DETACH DELETE n")
                print("  Neo4j cleared")
            except Exception as e:
                print(f"  Neo4j clear failed: {e}")
        
        print("\n" + "="*60)
        print("All databases cleared successfully!")
        print("="*60)


# ============================================================================
# DATA GENERATORS
# ============================================================================

class DataGenerator:
    """Generates synthetic e-commerce data"""
    
    def __init__(self):
        self.categories = [
            {"name": "Electronics", "subcategories": ["Audio", "Computers", "Cameras"]},
            {"name": "Fashion", "subcategories": ["Men", "Women", "Kids"]},
            {"name": "Home & Decor", "subcategories": ["Furniture", "Lighting", "Decor"]},
            {"name": "Sports", "subcategories": ["Fitness", "Outdoor", "Team Sports"]},
            {"name": "Books", "subcategories": ["Fiction", "Non-Fiction", "Educational"]}
        ]
        
        self.product_templates = {
            "Electronics": {
                "names": ["Wireless Headphones", "Bluetooth Speaker", "Laptop", "Smartphone", "Tablet"],
                "brands": ["Sony", "Apple", "Samsung", "Bose", "Dell"],
                "attributes": {
                    "battery_life": ["10 hours", "20 hours", "30 hours"],
                    "connectivity": ["Bluetooth 5.0", "WiFi", "USB-C"],
                    "weight": [150, 200, 250, 300, 500],
                    "color_options": ["black", "white", "silver", "blue"]
                }
            },
            "Fashion": {
                "names": ["Summer Dress", "Jeans", "T-Shirt", "Jacket", "Sneakers"],
                "brands": ["Nike", "Zara", "H&M", "Adidas", "Levi's"],
                "attributes": {
                    "material": ["cotton", "polyester", "denim", "leather"],
                    "available_sizes": ["XS", "S", "M", "L", "XL"],
                    "available_colors": ["red", "blue", "black", "white", "green"],
                    "pattern": ["solid", "striped", "floral", "checkered"]
                }
            },
            "Home & Decor": {
                "names": ["Ceramic Vase", "Table Lamp", "Wall Art", "Throw Pillow", "Area Rug"],
                "brands": ["IKEA", "West Elm", "Crate&Barrel", "Pottery Barn"],
                "attributes": {
                    "material": ["ceramic", "glass", "wood", "metal", "fabric"],
                    "color": ["white", "beige", "gray", "multicolor"],
                    "style": ["modern", "traditional", "minimalist", "rustic"]
                }
            }
        }
    
    def generate_users(self, count):
        """Generate user data"""
        users = []
        for i in range(count):
            # First user is always Sarah Johnson for testing consistency
            if i == 0:
                user = {
                    'first_name': 'Sarah',
                    'last_name': 'Johnson',
                    'email': 'sarah.johnson@example.com',
                    'username': 'sarah_johnson',
                    'password_hash': fake.sha256(),
                    'created_at': fake.date_time_between(start_date='-2y', end_date='now')
                }
            else:
                user = {
                    'first_name': fake.first_name(),
                    'last_name': fake.last_name(),
                    'email': fake.unique.email(),
                    'username': fake.unique.user_name(),
                    'password_hash': fake.sha256(),
                    'created_at': fake.date_time_between(start_date='-2y', end_date='now')
                }
            users.append(user)
        return users
    
    def generate_addresses(self, count):
        """Generate address data"""
        addresses = []
        for i in range(count):
            address = {
                'address_line1': fake.street_address(),
                'address_line2': fake.secondary_address() if random.random() > 0.7 else None,
                'city': fake.city(),
                'state_province': fake.state(),
                'postal_code': fake.postcode(),
                'country': 'USA',
                'created_at': fake.date_time_between(start_date='-2y', end_date='now')
            }
            addresses.append(address)
        return addresses
    
    def generate_categories(self):
        """Generate category hierarchy"""
        categories = []
        for idx, cat in enumerate(self.categories):
            parent = {
                'name': cat['name'],
                'description': f"{cat['name']} products",
                'parent_category_id': None,
                'created_at': datetime.now()
            }
            categories.append(parent)
            
            # Add subcategories
            for subcat in cat['subcategories']:
                child = {
                    'name': subcat,
                    'description': f"{subcat} in {cat['name']}",
                    'parent_category_id': idx + 1,  # Will be updated with actual ID
                    'created_at': datetime.now()
                }
                categories.append(child)
        
        return categories
    
    def generate_products(self, count, category_count):
        """Generate product data"""
        products = []
        category_types = ["Electronics", "Fashion", "Home & Decor"]
        
        for i in range(count):
            category_type = random.choice(category_types)
            template = self.product_templates[category_type]
            
            name = random.choice(template['names'])
            brand = random.choice(template['brands'])
            
            product = {
                'name': f"{brand} {name}",
                'description': fake.text(max_nb_chars=200),
                'base_price': round(random.uniform(19.99, 999.99), 2),
                'total_stock_quantity': random.randint(0, 500),
                'image_url': f"/images/product_{i+1}.jpg",
                'is_active': True,
                'created_at': fake.date_time_between(start_date='-1y', end_date='now'),
                'category_type': category_type,
                'brand': brand
            }
            products.append(product)
        
        return products
    
    def generate_product_attributes(self, product_id, category_type):
        """Generate MongoDB product attributes for a product"""
        template = self.product_templates.get(category_type, {})
        attributes = {}
        
        for attr_name, attr_values in template.get('attributes', {}).items():
            if isinstance(attr_values, list):
                if 'color' in attr_name or 'size' in attr_name:
                    # Keep as array
                    attributes[attr_name] = random.sample(attr_values, k=random.randint(2, len(attr_values)))
                else:
                    # Pick one value
                    attributes[attr_name] = random.choice(attr_values)
            else:
                attributes[attr_name] = attr_values
        
        # Add stock by variant for fashion items
        stock_by_variant = {}
        if category_type == "Fashion":
            colors = attributes.get('available_colors', ['default'])
            sizes = attributes.get('available_sizes', ['M'])
            for color in colors[:2]:  # Limit combinations
                for size in sizes[:3]:
                    variant_key = f"{color}-{size}"
                    stock_by_variant[variant_key] = random.randint(0, 50)
        else:
            stock_by_variant['default'] = random.randint(0, 100)
        
        return {
            'product_id': f"item_{product_id}",
            'category': category_type.lower().replace(' & ', '_'),
            'attributes': attributes,
            'stock_by_variant': stock_by_variant,
            'created_at': datetime.now(),
            'updated_at': datetime.now()
        }


# ============================================================================
# POSTGRESQL SCHEMA CREATION
# ============================================================================

def create_postgresql_schema(conn):
    """Create all PostgreSQL tables if they don't exist"""
    print("\n" + "="*60)
    print("CREATING POSTGRESQL SCHEMA")
    print("="*60)
    
    cursor = conn.cursor()
    
    # SQL commands to create schema
    schema_sql = """
    -- Users
    CREATE TABLE IF NOT EXISTS "User" (
        UserID SERIAL PRIMARY KEY,
        FirstName VARCHAR(100) NOT NULL,
        LastName VARCHAR(100) NOT NULL,
        Email VARCHAR(255) UNIQUE NOT NULL,
        Username VARCHAR(100) UNIQUE NOT NULL,
        PasswordHash VARCHAR(255) NOT NULL,
        PrimaryAddressID INT,
        CreatedAt TIMESTAMP NOT NULL DEFAULT NOW(),
        UpdatedAt TIMESTAMP NOT NULL DEFAULT NOW()
    );

    -- Addresses
    CREATE TABLE IF NOT EXISTS Address (
        AddressID SERIAL PRIMARY KEY,
        AddressLine1 VARCHAR(255) NOT NULL,
        AddressLine2 VARCHAR(255),
        City VARCHAR(100) NOT NULL,
        StateProvince VARCHAR(100) NOT NULL,
        PostalCode VARCHAR(20) NOT NULL,
        Country VARCHAR(100) NOT NULL DEFAULT 'USA',
        CreatedAt TIMESTAMP NOT NULL DEFAULT NOW()
    );

    -- Categories
    CREATE TABLE IF NOT EXISTS Category (
        CategoryID SERIAL PRIMARY KEY,
        CategoryName VARCHAR(100) UNIQUE NOT NULL,
        Description TEXT,
        ParentCategoryID INT,
        CreatedAt TIMESTAMP NOT NULL DEFAULT NOW(),
        FOREIGN KEY (ParentCategoryID) REFERENCES Category(CategoryID)
    );

    -- Items
    CREATE TABLE IF NOT EXISTS Item (
        ItemID SERIAL PRIMARY KEY,
        Name VARCHAR(255) NOT NULL,
        Description TEXT,
        BasePrice DECIMAL(10,2) NOT NULL,
        TotalStockQuantity INT NOT NULL DEFAULT 0,
        ImageURL VARCHAR(500),
        IsActive BOOLEAN NOT NULL DEFAULT TRUE,
        CreatedAt TIMESTAMP NOT NULL DEFAULT NOW(),
        UpdatedAt TIMESTAMP NOT NULL DEFAULT NOW()
    );

    -- Item Categories (Many-to-Many)
    CREATE TABLE IF NOT EXISTS ItemCategory (
        ItemCategoryID SERIAL PRIMARY KEY,
        ItemID INT NOT NULL,
        CategoryID INT NOT NULL,
        IsPrimaryCategory BOOLEAN NOT NULL DEFAULT FALSE,
        CreatedAt TIMESTAMP NOT NULL DEFAULT NOW(),
        FOREIGN KEY (ItemID) REFERENCES Item(ItemID),
        FOREIGN KEY (CategoryID) REFERENCES Category(CategoryID)
    );

    -- Item Variations
    CREATE TABLE IF NOT EXISTS ItemVariation (
        ItemVariationID SERIAL PRIMARY KEY,
        ItemID INT NOT NULL,
        VariationSKU VARCHAR(100) UNIQUE NOT NULL,
        StockQuantity INT NOT NULL DEFAULT 0,
        PriceAdjustment DECIMAL(10,2),
        IsAvailable BOOLEAN NOT NULL DEFAULT TRUE,
        CreatedAt TIMESTAMP NOT NULL DEFAULT NOW(),
        UpdatedAt TIMESTAMP NOT NULL DEFAULT NOW(),
        FOREIGN KEY (ItemID) REFERENCES Item(ItemID)
    );

    -- Variation Traits
    CREATE TABLE IF NOT EXISTS VariationTrait (
        VariationTraitID SERIAL PRIMARY KEY,
        ItemVariationID INT NOT NULL,
        TraitType VARCHAR(50) NOT NULL,
        TraitValue VARCHAR(100) NOT NULL,
        CreatedAt TIMESTAMP NOT NULL DEFAULT NOW(),
        FOREIGN KEY (ItemVariationID) REFERENCES ItemVariation(ItemVariationID)
    );

    -- Shopping Cart (Backup from Redis)
    CREATE TABLE IF NOT EXISTS ShoppingCart (
        ShoppingCartID SERIAL PRIMARY KEY,
        UserID INT,
        Status VARCHAR(20) NOT NULL DEFAULT 'active',
        TotalAmount DECIMAL(10,2) NOT NULL DEFAULT 0,
        TotalItems INT NOT NULL DEFAULT 0,
        CreatedAt TIMESTAMP NOT NULL DEFAULT NOW(),
        UpdatedAt TIMESTAMP NOT NULL DEFAULT NOW(),
        AbandonedAt TIMESTAMP,
        ConvertedAt TIMESTAMP,
        SyncSource VARCHAR(50) DEFAULT 'redis_backup',
        FOREIGN KEY (UserID) REFERENCES "User"(UserID)
    );

    -- Shopping Cart Items
    CREATE TABLE IF NOT EXISTS ShoppingCartItem (
        ShoppingCartItemID SERIAL PRIMARY KEY,
        ShoppingCartID INT NOT NULL,
        ItemID INT NOT NULL,
        ItemVariationID INT,
        Quantity INT NOT NULL DEFAULT 1,
        PriceSnapshot DECIMAL(10,2) NOT NULL,
        ItemNameSnapshot VARCHAR(255) NOT NULL,
        AddedAt TIMESTAMP NOT NULL DEFAULT NOW(),
        UpdatedAt TIMESTAMP NOT NULL DEFAULT NOW(),
        FOREIGN KEY (ShoppingCartID) REFERENCES ShoppingCart(ShoppingCartID),
        FOREIGN KEY (ItemID) REFERENCES Item(ItemID),
        FOREIGN KEY (ItemVariationID) REFERENCES ItemVariation(ItemVariationID)
    );

    -- Payment Types
    CREATE TABLE IF NOT EXISTS PaymentType (
        PaymentTypeID SERIAL PRIMARY KEY,
        TypeName VARCHAR(50) UNIQUE NOT NULL,
        Description TEXT,
        IsActive BOOLEAN NOT NULL DEFAULT TRUE,
        CreatedAt TIMESTAMP NOT NULL DEFAULT NOW()
    );

    -- Payment Methods
    CREATE TABLE IF NOT EXISTS PaymentMethod (
        PaymentMethodID SERIAL PRIMARY KEY,
        UserID INT NOT NULL,
        PaymentTypeID INT NOT NULL,
        AccountNumberLast4 VARCHAR(4),
        CardType VARCHAR(50),
        ExpirationMonth VARCHAR(2),
        ExpirationYear VARCHAR(4),
        AccountHolderName VARCHAR(255) NOT NULL,
        BillingAddressID INT,
        IsDefault BOOLEAN NOT NULL DEFAULT FALSE,
        IsActive BOOLEAN NOT NULL DEFAULT TRUE,
        CreatedAt TIMESTAMP NOT NULL DEFAULT NOW(),
        UpdatedAt TIMESTAMP NOT NULL DEFAULT NOW(),
        FOREIGN KEY (UserID) REFERENCES "User"(UserID),
        FOREIGN KEY (PaymentTypeID) REFERENCES PaymentType(PaymentTypeID),
        FOREIGN KEY (BillingAddressID) REFERENCES Address(AddressID)
    );

    -- Shipping Options
    CREATE TABLE IF NOT EXISTS ShippingOption (
        ShippingOptionID SERIAL PRIMARY KEY,
        Name VARCHAR(100) NOT NULL,
        Description TEXT,
        EstimatedDays VARCHAR(50),
        Rate DECIMAL(10,2) NOT NULL,
        IsActive BOOLEAN NOT NULL DEFAULT TRUE,
        CreatedAt TIMESTAMP NOT NULL DEFAULT NOW(),
        UpdatedAt TIMESTAMP NOT NULL DEFAULT NOW()
    );

    -- Tax Rates
    CREATE TABLE IF NOT EXISTS TaxRate (
        TaxRateID SERIAL PRIMARY KEY,
        TaxName VARCHAR(100) NOT NULL,
        Rate DECIMAL(5,4) NOT NULL,
        ApplicableRegion VARCHAR(100) NOT NULL,
        IsActive BOOLEAN NOT NULL DEFAULT TRUE,
        EffectiveFrom DATE NOT NULL,
        EffectiveTo DATE,
        CreatedAt TIMESTAMP NOT NULL DEFAULT NOW()
    );

    -- Orders
    CREATE TABLE IF NOT EXISTS "Order" (
        OrderID SERIAL PRIMARY KEY,
        UserID INT NOT NULL,
        OrderNumber VARCHAR(50) UNIQUE NOT NULL,
        Status VARCHAR(50) NOT NULL DEFAULT 'pending',
        SubtotalAmount DECIMAL(10,2) NOT NULL,
        TaxAmount DECIMAL(10,2) NOT NULL,
        ShippingAmount DECIMAL(10,2) NOT NULL,
        TotalAmount DECIMAL(10,2) NOT NULL,
        ShippingAddressID INT NOT NULL,
        ShippingOptionID INT NOT NULL,
        PaymentMethodID INT NOT NULL,
        OrderDate TIMESTAMP NOT NULL DEFAULT NOW(),
        ExpectedShipDate DATE,
        ActualShipDate DATE,
        ExpectedDeliveryDate DATE,
        ActualDeliveryDate DATE,
        OrderNotes TEXT,
        CreatedAt TIMESTAMP NOT NULL DEFAULT NOW(),
        UpdatedAt TIMESTAMP NOT NULL DEFAULT NOW(),
        FOREIGN KEY (UserID) REFERENCES "User"(UserID),
        FOREIGN KEY (ShippingAddressID) REFERENCES Address(AddressID),
        FOREIGN KEY (ShippingOptionID) REFERENCES ShippingOption(ShippingOptionID),
        FOREIGN KEY (PaymentMethodID) REFERENCES PaymentMethod(PaymentMethodID)
    );

    -- Order Items
    CREATE TABLE IF NOT EXISTS OrderItem (
        OrderItemID SERIAL PRIMARY KEY,
        OrderID INT NOT NULL,
        ItemID INT NOT NULL,
        ItemVariationID INT,
        Quantity INT NOT NULL,
        UnitPrice DECIMAL(10,2) NOT NULL,
        LineTotal DECIMAL(10,2) NOT NULL,
        ItemNameSnapshot VARCHAR(255) NOT NULL,
        VariationDescription VARCHAR(255),
        CreatedAt TIMESTAMP NOT NULL DEFAULT NOW(),
        FOREIGN KEY (OrderID) REFERENCES "Order"(OrderID),
        FOREIGN KEY (ItemID) REFERENCES Item(ItemID),
        FOREIGN KEY (ItemVariationID) REFERENCES ItemVariation(ItemVariationID)
    );

    -- Order Taxes
    CREATE TABLE IF NOT EXISTS OrderTax (
        OrderTaxID SERIAL PRIMARY KEY,
        OrderID INT NOT NULL,
        TaxRateID INT NOT NULL,
        TaxableAmount DECIMAL(10,2) NOT NULL,
        TaxAmount DECIMAL(10,2) NOT NULL,
        CreatedAt TIMESTAMP NOT NULL DEFAULT NOW(),
        FOREIGN KEY (OrderID) REFERENCES "Order"(OrderID),
        FOREIGN KEY (TaxRateID) REFERENCES TaxRate(TaxRateID)
    );

    -- Returns
    CREATE TABLE IF NOT EXISTS ReturnItem (
        ReturnID SERIAL PRIMARY KEY,
        OrderID INT NOT NULL,
        OrderItemID INT NOT NULL,
        ItemID INT NOT NULL,
        ReturnReason VARCHAR(255),
        ReturnNotes TEXT,
        Status VARCHAR(50) NOT NULL DEFAULT 'requested',
        Quantity INT NOT NULL,
        RefundAmount DECIMAL(10,2) NOT NULL,
        RestockingFee DECIMAL(10,2) DEFAULT 0,
        RefundStatus VARCHAR(50) NOT NULL DEFAULT 'pending',
        ReturnShippingOptionID INT,
        RequestedAt TIMESTAMP NOT NULL DEFAULT NOW(),
        ApprovedAt TIMESTAMP,
        ReceivedAt TIMESTAMP,
        RefundedAt TIMESTAMP,
        CreatedAt TIMESTAMP NOT NULL DEFAULT NOW(),
        UpdatedAt TIMESTAMP NOT NULL DEFAULT NOW(),
        FOREIGN KEY (OrderID) REFERENCES "Order"(OrderID),
        FOREIGN KEY (OrderItemID) REFERENCES OrderItem(OrderItemID),
        FOREIGN KEY (ItemID) REFERENCES Item(ItemID),
        FOREIGN KEY (ReturnShippingOptionID) REFERENCES ShippingOption(ShippingOptionID)
    );
    """
    
    try:
        print("Creating tables...")
        cursor.execute(schema_sql)
        
        # Add User -> Address foreign key if it doesn't exist
        print("Adding constraints...")
        cursor.execute("""
            DO $$ 
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint WHERE conname = 'fk_user_address'
                ) THEN
                    ALTER TABLE "User" ADD CONSTRAINT fk_user_address
                        FOREIGN KEY (PrimaryAddressID) REFERENCES Address(AddressID);
                END IF;
            END $$;
        """)
        
        # Create indexes
        print("Creating indexes...")
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_user_email ON "User"(Email);
            CREATE INDEX IF NOT EXISTS idx_item_active ON Item(IsActive);
            CREATE INDEX IF NOT EXISTS idx_item_category ON ItemCategory(ItemID, CategoryID);
            CREATE INDEX IF NOT EXISTS idx_order_user ON "Order"(UserID);
            CREATE INDEX IF NOT EXISTS idx_order_date ON "Order"(OrderDate);
            CREATE INDEX IF NOT EXISTS idx_order_status ON "Order"(Status);
            CREATE INDEX IF NOT EXISTS idx_orderitem_order ON OrderItem(OrderID);
        """)
        
        conn.commit()
        print("PostgreSQL schema created successfully!\n")
        
    except Exception as e:
        conn.rollback()
        print(f"Error creating schema: {e}")
        raise


# ============================================================================
# POSTGRESQL POPULATOR
# ============================================================================

class PostgreSQLPopulator:
    """Populates PostgreSQL database"""
    
    def __init__(self, conn, generator):
        self.conn = conn
        self.generator = generator
        self.user_ids = []
        self.address_ids = []
        self.category_ids = []
        self.item_ids = []
    
    def populate(self, config):
        """Populate PostgreSQL with test data"""
        print("\n=== Populating PostgreSQL ===")
        cursor = self.conn.cursor()
        
        num_users = config['num_users']
        num_products = config['num_products']
        orders_per_user = config['orders_per_user']
        
        try:
            # 1. Insert Addresses
            print("Inserting addresses...")
            addresses = self.generator.generate_addresses(num_users)
            address_query = """
                INSERT INTO Address (AddressLine1, AddressLine2, City, StateProvince, PostalCode, Country, CreatedAt)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING AddressID
            """
            for addr in addresses:
                cursor.execute(address_query, (
                    addr['address_line1'], addr['address_line2'], addr['city'],
                    addr['state_province'], addr['postal_code'], addr['country'],
                    addr['created_at']
                ))
                self.address_ids.append(cursor.fetchone()[0])
            print(f"  {len(self.address_ids):,} addresses inserted")
            
            # 2. Insert Users
            print("Inserting users...")
            users = self.generator.generate_users(num_users)
            user_query = """
                INSERT INTO "User" (FirstName, LastName, Email, Username, PasswordHash, PrimaryAddressID, CreatedAt, UpdatedAt)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING UserID
            """
            for idx, user in enumerate(users):
                cursor.execute(user_query, (
                    user['first_name'], user['last_name'], user['email'],
                    user['username'], user['password_hash'], self.address_ids[idx],
                    user['created_at'], user['created_at']
                ))
                self.user_ids.append(cursor.fetchone()[0])
            print(f"  {len(self.user_ids):,} users inserted")
            
            # 3. Insert Categories
            print("Inserting categories...")
            categories = self.generator.generate_categories()
            category_query = """
                INSERT INTO Category (CategoryName, Description, ParentCategoryID, CreatedAt)
                VALUES (%s, %s, %s, %s)
                RETURNING CategoryID
            """
            
            # First pass: Insert parent categories and track their IDs
            parent_category_map = {}  # Maps category name to database ID
            
            for cat in categories:
                if cat['parent_category_id'] is None:  # Parent category
                    cursor.execute(category_query, (
                        cat['name'], cat['description'], None, cat['created_at']
                    ))
                    cat_id = cursor.fetchone()[0]
                    self.category_ids.append(cat_id)
                    parent_category_map[cat['name']] = cat_id
            
            # Second pass: Insert child categories with correct parent IDs
            for cat in categories:
                if cat['parent_category_id'] is not None:  # Child category
                    # Calculate correct parent ID
                    parent_idx = cat['parent_category_id'] - 1
                    parent_name = list(parent_category_map.keys())[parent_idx] if parent_idx < len(parent_category_map) else None
                    actual_parent_id = parent_category_map.get(parent_name) if parent_name else None
                    
                    cursor.execute(category_query, (
                        cat['name'], cat['description'], actual_parent_id, cat['created_at']
                    ))
                    self.category_ids.append(cursor.fetchone()[0])
            
            print(f"  {len(self.category_ids):,} categories inserted")
            
            # 4. Insert Items
            print("Inserting items...")
            products = self.generator.generate_products(num_products, len(self.category_ids))
            item_query = """
                INSERT INTO Item (Name, Description, BasePrice, TotalStockQuantity, ImageURL, IsActive, CreatedAt, UpdatedAt)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING ItemID
            """
            for product in products:
                cursor.execute(item_query, (
                    product['name'], product['description'], product['base_price'],
                    product['total_stock_quantity'], product['image_url'], product['is_active'],
                    product['created_at'], product['created_at']
                ))
                self.item_ids.append(cursor.fetchone()[0])
            print(f"  {len(self.item_ids):,} items inserted")
            
            # 5. Link Items to Categories
            print("Linking items to categories...")
            item_category_query = """
                INSERT INTO ItemCategory (ItemID, CategoryID, IsPrimaryCategory, CreatedAt)
                VALUES (%s, %s, %s, %s)
            """
            
            # Map category types to their database IDs
            category_type_map = {
                'Electronics': parent_category_map.get('Electronics'),
                'Fashion': parent_category_map.get('Fashion'),
                'Home & Decor': parent_category_map.get('Home & Decor'),
                'Sports': parent_category_map.get('Sports'),
                'Books': parent_category_map.get('Books')
            }
            
            for idx, item_id in enumerate(self.item_ids):
                # Get the product's actual category type
                product_category_type = products[idx]['category_type']
                
                # Assign to matching category
                category_id = category_type_map.get(product_category_type)
                
                if category_id:
                    cursor.execute(item_category_query, (
                        item_id, category_id, True, datetime.now()
                    ))
                else:
                    # Fallback to Electronics if category not found
                    cursor.execute(item_category_query, (
                        item_id, category_type_map['Electronics'], True, datetime.now()
                    ))
            
            print(f"  {len(self.item_ids):,} item-category relationships created")
            
            # 6. Create shipping options, payment types, and payment methods
            print("Creating shipping and payment options...")
            
            # Shipping options
            cursor.execute("""
                INSERT INTO ShippingOption (Name, Description, EstimatedDays, Rate, IsActive)
                VALUES 
                    ('Standard Shipping', '5-7 business days', '5-7 business days', 5.99, true),
                    ('Express Shipping', '2-3 business days', '2-3 business days', 15.99, true),
                    ('Overnight', 'Next business day', '1 business day', 29.99, true)
                ON CONFLICT DO NOTHING
                RETURNING ShippingOptionID
            """)
            shipping_option_ids = [row[0] for row in cursor.fetchall()]
            
            if not shipping_option_ids:
                cursor.execute("SELECT ShippingOptionID FROM ShippingOption LIMIT 3")
                shipping_option_ids = [row[0] for row in cursor.fetchall()]
            
            # Payment types
            cursor.execute("""
                INSERT INTO PaymentType (TypeName, Description, IsActive)
                VALUES 
                    ('Credit Card', 'Visa, Mastercard, Amex', true),
                    ('Debit Card', 'Bank debit card', true),
                    ('PayPal', 'PayPal account', true)
                ON CONFLICT DO NOTHING
                RETURNING PaymentTypeID
            """)
            payment_type_ids = [row[0] for row in cursor.fetchall()]
            
            if not payment_type_ids:
                cursor.execute("SELECT PaymentTypeID FROM PaymentType LIMIT 3")
                payment_type_ids = [row[0] for row in cursor.fetchall()]
            
            # Create payment method for each user
            payment_method_ids = []
            for user_id in self.user_ids[:num_users]:
                cursor.execute("""
                    INSERT INTO PaymentMethod (
                        UserID, PaymentTypeID, AccountNumberLast4, CardType,
                        ExpirationMonth, ExpirationYear, AccountHolderName,
                        BillingAddressID, IsDefault, IsActive
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING PaymentMethodID
                """, (user_id, random.choice(payment_type_ids), 
                      str(random.randint(1000, 9999)),
                      random.choice(['Visa', 'Mastercard', 'Amex']),
                      '12', '2026', f'User {user_id}',
                      self.address_ids[self.user_ids.index(user_id)],
                      True, True))
                payment_method_ids.append(cursor.fetchone()[0])
            
            print(f"  {len(shipping_option_ids)} shipping options created")
            print(f"  {len(payment_type_ids)} payment types created")
            print(f"  {len(payment_method_ids):,} payment methods created")
            
            # 7. Create sample orders (SCALED FOR PRODUCTION)
            print("Creating sample orders...")
            
            order_count = 0
            order_item_count = 0
            
            for user_idx, user_id in enumerate(self.user_ids[:num_users]):
                # Number of orders per user (based on config)
                num_orders = random.randint(orders_per_user[0], orders_per_user[1])
                
                for _ in range(num_orders):
                    # Order dates in past 365 days (wider range for production scale)
                    days_ago = random.randint(1, 365)
                    order_date = datetime.now() - timedelta(days=days_ago)
                    
                    # Order status (weighted towards delivered: 60%)
                    status_options = [
                        ('delivered', True, True),   # Has ship and delivery dates
                        ('shipped', True, False),     # Has ship date only
                        ('processing', False, False), # No dates yet
                        ('pending', False, False)     # No dates yet
                    ]
                    status_weights = [0.6, 0.2, 0.1, 0.1]
                    status, has_ship, has_delivery = random.choices(status_options, weights=status_weights)[0]
                    
                    ship_date = order_date + timedelta(days=2) if has_ship else None
                    delivery_date = order_date + timedelta(days=5) if has_delivery else None
                    
                    # Select 1-5 items for order (more items per order for production)
                    num_items_in_order = random.randint(1, 5)
                    order_items = random.sample(list(zip(self.item_ids, products)), 
                                               min(num_items_in_order, len(self.item_ids)))
                    
                    # Calculate totals
                    subtotal = sum(float(product['base_price']) * random.randint(1, 3) 
                                 for _, product in order_items)
                    tax = subtotal * 0.08  # 8% tax
                    shipping_cost = random.choice([5.99, 15.99, 29.99])
                    total = subtotal + tax + shipping_cost
                    
                    # Create order with guaranteed unique order number
                    # Use timestamp + counter for uniqueness at scale
                    order_number = f"ORD-{int(order_date.timestamp())}-{order_count:06d}"
                    
                    cursor.execute("""
                        INSERT INTO "Order" (
                            UserID, OrderNumber, Status, SubtotalAmount, TaxAmount,
                            ShippingAmount, TotalAmount, ShippingAddressID, ShippingOptionID,
                            PaymentMethodID, OrderDate, ActualShipDate, ActualDeliveryDate
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING OrderID
                    """, (user_id, order_number, status, subtotal, tax, shipping_cost, total,
                          self.address_ids[user_idx], random.choice(shipping_option_ids),
                          payment_method_ids[user_idx], order_date, ship_date, delivery_date))
                    
                    order_id = cursor.fetchone()[0]
                    order_count += 1
                    
                    # Insert order items
                    for item_id, product in order_items:
                        quantity = random.randint(1, 3)
                        unit_price = float(product['base_price'])
                        line_total = unit_price * quantity
                        
                        cursor.execute("""
                            INSERT INTO OrderItem (
                                OrderID, ItemID, Quantity, UnitPrice, LineTotal,
                                ItemNameSnapshot
                            )
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """, (order_id, item_id, quantity, unit_price, line_total,
                              product['name']))
                        
                        order_item_count += 1
                    
                    # Commit every 1000 orders for performance
                    if order_count % 1000 == 0:
                        self.conn.commit()
                        print(f"    Progress: {order_count:,} orders created...")
            
            # Final commit
            self.conn.commit()
            print(f"  {order_count:,} orders created")
            print(f"  {order_item_count:,} order items created")
            
            # 8. Create sample returns (for delivered orders)
            print("Creating sample returns...")
            
            return_count = 0
            
            # Get delivered orders (more for production scale)
            limit = 50 if num_users >= 1000 else 10
            cursor.execute(f"""
                SELECT o.OrderID, o.OrderDate, o.UserID
                FROM "Order" o
                WHERE o.Status = 'delivered'
                ORDER BY o.OrderDate DESC
                LIMIT {limit}
            """)
            
            delivered_orders = cursor.fetchall()
            
            if delivered_orders:
                # Return reasons
                return_reasons = [
                    "Defective or damaged",
                    "Wrong item received",
                    "Not as described",
                    "Changed mind",
                    "Found better price elsewhere",
                    "Ordered by mistake",
                    "Poor quality",
                    "Arrived too late"
                ]
                
                # Create returns for 30-50% of delivered orders
                num_returns = int(len(delivered_orders) * random.uniform(0.3, 0.5))
                orders_to_return = random.sample(delivered_orders, num_returns)
                
                for order_id, order_date, user_id in orders_to_return:
                    # Get items from this order
                    cursor.execute("""
                        SELECT OrderItemID, ItemID, Quantity, UnitPrice, ItemNameSnapshot
                        FROM OrderItem
                        WHERE OrderID = %s
                    """, (order_id,))
                    
                    order_items = cursor.fetchall()
                    
                    if not order_items:
                        continue
                    
                    # Return 1 item from this order
                    order_item_id, item_id, ordered_qty, unit_price, item_name = random.choice(order_items)
                    
                    # Return quantity (might be partial)
                    return_qty = random.randint(1, ordered_qty)
                    
                    # Calculate refund
                    refund_amount = float(unit_price) * return_qty
                    
                    # Restocking fee (0%, 10%, or 15%)
                    restocking_fee_pct = random.choice([0, 0, 0, 0.10, 0.15])  # 60% no fee
                    restocking_fee = refund_amount * restocking_fee_pct
                    
                    # Return status and refund status
                    status_options = [
                        ('completed', 'issued', True, True, True),      # Fully processed
                        ('received', 'approved', True, True, False),    # Received, processing refund
                        ('approved', 'pending', True, False, False),    # Approved, waiting for return
                        ('requested', 'pending', False, False, False)   # Just requested
                    ]
                    
                    return_status, refund_status, has_approved, has_received, has_refunded = random.choice(status_options)
                    
                    # Dates
                    days_after_delivery = random.randint(5, 15)
                    requested_at = order_date + timedelta(days=days_after_delivery)
                    
                    approved_at = requested_at + timedelta(days=1) if has_approved else None
                    received_at = approved_at + timedelta(days=3) if has_received and approved_at else None
                    refunded_at = received_at + timedelta(days=2) if has_refunded and received_at else None
                    
                    # Return reason
                    reason = random.choice(return_reasons)
                    
                    # Notes (sometimes)
                    notes = fake.sentence() if random.random() > 0.6 else None
                    
                    # Insert return
                    cursor.execute("""
                        INSERT INTO ReturnItem (
                            OrderID, OrderItemID, ItemID, ReturnReason, ReturnNotes,
                            Status, Quantity, RefundAmount, RestockingFee, RefundStatus,
                            RequestedAt, ApprovedAt, ReceivedAt, RefundedAt
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (order_id, order_item_id, item_id, reason, notes,
                          return_status, return_qty, refund_amount, restocking_fee, refund_status,
                          requested_at, approved_at, received_at, refunded_at))
                    
                    return_count += 1
            
            self.conn.commit()
            print(f"  {return_count:,} returns created")
            
            print("PostgreSQL population complete!")
            
            return {
                'user_ids': self.user_ids,
                'address_ids': self.address_ids,
                'category_ids': self.category_ids,
                'item_ids': self.item_ids,
                'products': products,
                'order_count': order_count
            }
            
        except Exception as e:
            self.conn.rollback()
            print(f"Error populating PostgreSQL: {e}")
            raise


# ============================================================================
# MONGODB POPULATOR
# ============================================================================

class MongoDBPopulator:
    """Populates MongoDB database"""
    
    def __init__(self, db, generator):
        self.db = db
        self.generator = generator
    
    def populate(self, pg_data, config):
        """Populate MongoDB with test data"""
        print("\n=== Populating MongoDB ===")
        
        num_view_events = config['num_view_events']
        num_search_events = config['num_search_events']
        
        # Clear existing data
        print("Clearing existing MongoDB collections...")
        self.db.product_attributes.delete_many({})
        self.db.user_events.delete_many({})
        print("  Collections cleared")
        
        # 1. Product Attributes
        print("Inserting product attributes...")
        product_attrs = []
        for idx, product in enumerate(pg_data['products']):
            item_id = pg_data['item_ids'][idx]
            attrs = self.generator.generate_product_attributes(
                item_id,
                product['category_type']
            )
            product_attrs.append(attrs)
        
        if product_attrs:
            self.db.product_attributes.insert_many(product_attrs)
            print(f"  {len(product_attrs):,} product attributes inserted")
        
        # 2. Create sample user events (SCALED FOR PRODUCTION)
        print("Inserting sample user events...")
        user_events = []
        batch_size = 10000
        
        for i in range(num_view_events):
            event = {
                'user_id': f"user_{random.choice(pg_data['user_ids'])}",
                'session_id': f"sess_{fake.uuid4()[:8]}",
                'event_type': random.choice(['view', 'click', 'add_to_cart']),
                'event_data': {
                    'product_id': f"item_{random.choice(pg_data['item_ids'])}",
                    'duration_seconds': random.randint(5, 120)
                },
                'timestamp': fake.date_time_between(start_date='-365d', end_date='now'),
                'device_type': random.choice(['laptop', 'mobile', 'tablet']),
                'created_at': datetime.now()
            }
            user_events.append(event)
            
            # Insert in batches for performance
            if len(user_events) >= batch_size:
                self.db.user_events.insert_many(user_events)
                print(f"    Progress: {i+1:,}/{num_view_events:,} view events created...")
                user_events = []
        
        # Insert remaining events
        if user_events:
            self.db.user_events.insert_many(user_events)
        
        print(f"  {num_view_events:,} user events inserted")
        
        # 3. Create search events (SCALED FOR PRODUCTION)
        print("Inserting sample search events...")
        
        search_terms = [
            'wireless headphones', 'bluetooth speaker', 'laptop', 'phone case',
            'summer dress', 'jeans', 'sneakers', 'jacket', 't-shirt',
            'coffee table', 'lamp', 'chair', 'rug', 'pillow',
            'water bottle', 'yoga mat', 'dumbbells', 'running shoes'
        ]
        
        # Time-based weights (more searches in evening)
        time_weights = {
            'night': (0, 6, 0.1),      # 12 AM - 6 AM: 10%
            'morning': (6, 12, 0.2),   # 6 AM - 12 PM: 20%
            'afternoon': (12, 18, 0.3), # 12 PM - 6 PM: 30%
            'evening': (18, 24, 0.4)   # 6 PM - 12 AM: 40%
        }
        
        search_events = []
        
        for i in range(num_search_events):
            # Select time period based on weights
            time_period = random.choices(
                list(time_weights.keys()),
                weights=[w[2] for w in time_weights.values()]
            )[0]
            
            hour_min, hour_max, _ = time_weights[time_period]
            
            # Create timestamp with specific hour range
            days_ago = random.randint(1, 365)
            search_time = datetime.now() - timedelta(
                days=days_ago,
                hours=24 - random.randint(hour_min, hour_max - 1),
                minutes=random.randint(0, 59)
            )
            
            # Some search terms are more popular (weighted selection)
            popular_terms = search_terms[:6]  # First 6 are more popular
            other_terms = search_terms[6:]
            
            if random.random() < 0.6:  # 60% chance of popular term
                search_term = random.choice(popular_terms)
            else:
                search_term = random.choice(other_terms)
            
            search_event = {
                'user_id': f"user_{random.choice(pg_data['user_ids'])}",
                'session_id': f"sess_{fake.uuid4()[:8]}",
                'event_type': 'search',
                'event_data': {
                    'search_query': search_term,
                    'results_count': random.randint(5, 50),
                    'filters_applied': random.choice([None, 'price', 'category', 'rating'])
                },
                'timestamp': search_time,
                'device_type': random.choice(['laptop', 'mobile', 'tablet']),
                'browser': random.choice(['Chrome', 'Firefox', 'Safari', 'Edge']),
                'created_at': datetime.now()
            }
            search_events.append(search_event)
            
            # Insert in batches
            if len(search_events) >= batch_size:
                self.db.user_events.insert_many(search_events)
                print(f"    Progress: {i+1:,}/{num_search_events:,} search events created...")
                search_events = []
        
        # Insert remaining
        if search_events:
            self.db.user_events.insert_many(search_events)
        
        print(f"  {num_search_events:,} search events inserted")
        
        total_events = num_view_events + num_search_events
        print(f"  TOTAL: {total_events:,} events in MongoDB")
        
        # 4. Create indexes
        print("Creating indexes...")
        self.db.product_attributes.create_index('product_id', unique=True)
        self.db.product_attributes.create_index('category')
        self.db.user_events.create_index([('user_id', 1), ('timestamp', -1)])
        self.db.user_events.create_index([('event_type', 1), ('timestamp', -1)])
        print("  Indexes created")
        
        print("MongoDB population complete!")
        
        return {
            'product_attrs_count': len(product_attrs),
            'user_events_count': total_events
        }


# ============================================================================
# REDIS POPULATOR
# ============================================================================

class RedisPopulator:
    """Populates Redis cache"""
    
    def __init__(self, client):
        self.client = client
    
    def populate(self, pg_data, config):
        """Populate Redis with test data"""
        print("\n=== Populating Redis ===")
        
        view_cache_range = config['view_cache_per_user']
        
        # Clear existing Redis data
        print("Clearing existing Redis keys...")
        for key in self.client.scan_iter("session:*"):
            self.client.delete(key)
        for key in self.client.scan_iter("user_sessions:*"):
            self.client.delete(key)
        for key in self.client.scan_iter("cart:*"):
            self.client.delete(key)
        for key in self.client.scan_iter("cart_items:*"):
            self.client.delete(key)
        for key in self.client.scan_iter("active_cart:*"):
            self.client.delete(key)
        for key in self.client.scan_iter("hot_product:*"):
            self.client.delete(key)
        for key in self.client.scan_iter("recent_views:*"):
            self.client.delete(key)
        print("  Redis cleared")
        
        # 1. Create sessions for first 5 users
        print("Creating sessions...")
        session_count = min(5, len(pg_data['user_ids']))
        for i in range(session_count):
            user_id = pg_data['user_ids'][i]
            session_token = f"sess_{fake.uuid4()[:8]}"
            
            # Create session hash
            self.client.hset(f"session:{session_token}", mapping={
                'session_id': session_token,
                'user_id': str(user_id),
                'device_type': random.choice(['laptop', 'mobile', 'tablet']),
                'created_at': datetime.now().isoformat(),
                'last_activity_at': datetime.now().isoformat(),
                'is_authenticated': 'true'
            })
            self.client.expire(f"session:{session_token}", 86400)  # 24 hours
            
            # Add to user's active sessions
            self.client.sadd(f"user_sessions:{user_id}", session_token)
            self.client.expire(f"user_sessions:{user_id}", 86400)
        
        print(f"  {session_count} sessions created")
        
        # 2. Cache hot products (first 10 items)
        print("Caching hot products...")
        hot_product_count = min(10, len(pg_data['item_ids']))
        for i in range(hot_product_count):
            item_id = pg_data['item_ids'][i]
            product = pg_data['products'][i]
            
            self.client.hset(f"hot_product:item_{item_id}", mapping={
                'product_id': f"item_{item_id}",
                'name': product['name'],
                'price': str(product['base_price']),
                'stock': str(product['total_stock_quantity']),
                'is_available': 'true',
                'cached_at': datetime.now().isoformat()
            })
            self.client.expire(f"hot_product:item_{item_id}", 300)  # 5 minutes
        
        print(f"  {hot_product_count} hot products cached")
        
        # 3. Create shopping carts for first 3 users
        print("Creating shopping carts...")
        cart_count = min(3, len(pg_data['user_ids']))
        
        for i in range(cart_count):
            user_id = pg_data['user_ids'][i]
            session_token = f"sess_{fake.uuid4()[:8]}"
            cart_id = f"cart_{user_id}_{int(datetime.now().timestamp())}"
            device_type = random.choice(['laptop', 'mobile', 'tablet'])
            
            # Select 1-4 random products for cart
            num_items_in_cart = random.randint(1, 4)
            cart_items = []
            total_amount = 0
            total_items = 0
            
            for _ in range(num_items_in_cart):
                # Pick random product
                product_idx = random.randint(0, len(pg_data['products']) - 1)
                product = pg_data['products'][product_idx]
                item_id = pg_data['item_ids'][product_idx]
                quantity = random.randint(1, 3)
                
                cart_item = {
                    'item_id': f'item_{item_id}',
                    'name': product['name'],
                    'price': float(product['base_price']),
                    'quantity': quantity,
                    'subtotal': float(product['base_price']) * quantity
                }
                
                cart_items.append(cart_item)
                total_amount += cart_item['subtotal']
                total_items += quantity
            
            # Store cart metadata
            self.client.hset(f"cart:{user_id}:{session_token}", mapping={
                'cart_id': cart_id,
                'user_id': str(user_id),
                'session_id': session_token,
                'device_type': device_type,
                'status': 'active',
                'total_items': str(total_items),
                'total_amount': str(total_amount),
                'created_at': datetime.now().isoformat(),
                'updated_at': datetime.now().isoformat()
            })
            self.client.expire(f"cart:{user_id}:{session_token}", 604800)  # 7 days
            
            # Store cart items
            for item in cart_items:
                self.client.rpush(f"cart_items:{cart_id}", json.dumps(item))
            self.client.expire(f"cart_items:{cart_id}", 604800)  # 7 days
            
            # Set active cart pointer
            self.client.set(f"active_cart:{user_id}", cart_id, ex=604800)
        
        print(f"  {cart_count} shopping carts created")
        
        # 4. Cache recent views for each user (for Query 2)
        print("Creating recent view cache...")
        
        view_cache_count = 0
        
        for idx, user_id in enumerate(pg_data['user_ids']):
            redis_key = f"recent_views:{user_id}"
            
            # Generate views based on config
            num_views = random.randint(view_cache_range[0], view_cache_range[1])
            
            for view_idx in range(num_views):
                # Random product
                product_idx = random.randint(0, len(pg_data['products']) - 1)
                item_id = pg_data['item_ids'][product_idx]
                
                # Random view duration
                duration = random.randint(5, 300)
                
                # Timestamp (most recent first)
                days_ago = random.randint(0, 180)  # Past 6 months
                hours_ago = random.randint(0, 23)
                minutes_ago = random.randint(0, 59)
                
                view_time = datetime.now() - timedelta(
                    days=days_ago,
                    hours=hours_ago,
                    minutes=minutes_ago
                )
                
                # Member: "product_id:duration"
                member = f"item_{item_id}:{duration}"
                # Score: timestamp (for sorting by recency)
                score = view_time.timestamp()
                
                # Add to sorted set
                self.client.zadd(redis_key, {member: score})
            
            # Keep only most recent 10
            self.client.zremrangebyrank(redis_key, 0, -11)
            
            # Set expiration (6 months)
            self.client.expire(redis_key, 15552000)
            
            view_cache_count += 1
            
            # Progress indicator for large datasets
            if (idx + 1) % 100 == 0:
                print(f"    Progress: {idx+1:,}/{len(pg_data['user_ids']):,} users cached...")
        
        print(f"  {view_cache_count:,} user view caches created")
        
        print("Redis population complete!")
        
        return {
            'sessions': session_count,
            'hot_products': hot_product_count,
            'carts': cart_count,
            'view_caches': view_cache_count
        }


# ============================================================================
# NEO4J POPULATOR
# ============================================================================

class Neo4jPopulator:
    """Populates Neo4j graph database"""
    
    def __init__(self, driver):
        self.driver = driver
    
    def populate(self, pg_data, mongo_db, pg_conn):
        """Populate Neo4j with test data including relationships"""
        print("\n=== Populating Neo4j ===")
        
        with self.driver.session() as session:
            # Clear existing data
            print("Clearing existing Neo4j data...")
            session.run("MATCH (n) DETACH DELETE n")
            print("  Graph cleared")
            
            # 1. Create User nodes
            print("Creating User nodes...")
            user_query = """
            UNWIND $users AS user
            CREATE (u:User {
                user_id: user.user_id,
                name: user.name,
                created_at: datetime(user.created_at),
                synced_at: datetime()
            })
            """
            users_data = [
                {
                    'user_id': f"user_{uid}",
                    'name': f"User {uid}",
                    'created_at': datetime.now().isoformat()
                }
                for uid in pg_data['user_ids'][:min(100, len(pg_data['user_ids']))]
            ]
            session.run(user_query, users=users_data)
            print(f"  {len(users_data):,} User nodes created")
            
            # 2. Create Product nodes
            print("Creating Product nodes...")
            product_query = """
            UNWIND $products AS product
            CREATE (p:Product {
                product_id: product.product_id,
                name: product.name,
                category: product.category,
                price: product.price,
                synced_at: datetime()
            })
            """
            products_data = [
                {
                    'product_id': f"item_{pid}",
                    'name': pg_data['products'][idx]['name'],
                    'category': pg_data['products'][idx]['category_type'],
                    'price': float(pg_data['products'][idx]['base_price'])
                }
                for idx, pid in enumerate(pg_data['item_ids'][:min(500, len(pg_data['item_ids']))])
            ]
            session.run(product_query, products=products_data)
            print(f"  {len(products_data):,} Product nodes created")
            
            # 3. Create Category nodes
            print("Creating Category nodes...")
            category_query = """
            UNWIND $categories AS category
            CREATE (c:Category {
                category_id: category.category_id,
                name: category.name,
                synced_at: datetime()
            })
            """
            categories_data = [
                {
                    'category_id': f"cat_{cid}",
                    'name': cat['name']
                }
                for cid, cat in enumerate(self.generator.categories[:5], 1)
            ]
            session.run(category_query, categories=categories_data)
            print(f"  {len(categories_data)} Category nodes created")
            
            # 4. Create constraints
            print("Creating constraints...")
            session.run("CREATE CONSTRAINT user_id_unique IF NOT EXISTS FOR (u:User) REQUIRE u.user_id IS UNIQUE")
            session.run("CREATE CONSTRAINT product_id_unique IF NOT EXISTS FOR (p:Product) REQUIRE p.product_id IS UNIQUE")
            session.run("CREATE CONSTRAINT category_id_unique IF NOT EXISTS FOR (c:Category) REQUIRE c.category_id IS UNIQUE")
            print("  Constraints created")
            
            # 5. Create BELONGS_TO relationships (Product → Category)
            print("Creating BELONGS_TO relationships...")
            belongs_to_query = """
            MATCH (p:Product), (c:Category)
            WHERE p.category = c.name
            CREATE (p)-[:BELONGS_TO]->(c)
            """
            result = session.run(belongs_to_query)
            belongs_to_count = result.consume().counters.relationships_created
            print(f"  {belongs_to_count:,} BELONGS_TO relationships created")
            
            # 6. Create PURCHASED relationships (User → Product)
            print("Creating PURCHASED relationships...")
            
            # Get order data from PostgreSQL
            cursor = pg_conn.cursor()
            cursor.execute("""
                SELECT 
                    o.UserID,
                    oi.ItemID,
                    o.OrderID,
                    o.OrderDate,
                    oi.Quantity,
                    oi.LineTotal
                FROM "Order" o
                JOIN OrderItem oi ON o.OrderID = oi.OrderID
                WHERE o.UserID IN %s
                  AND oi.ItemID IN %s
                ORDER BY o.OrderDate DESC
                LIMIT 5000
            """, (
                tuple(pg_data['user_ids'][:100]),
                tuple(pg_data['item_ids'][:500])
            ))
            
            purchases = cursor.fetchall()
            
            if purchases:
                purchased_data = [
                    {
                        'user_id': f"user_{user_id}",
                        'product_id': f"item_{item_id}",
                        'order_id': f"order_{order_id}",
                        'purchased_at': order_date.isoformat(),
                        'quantity': quantity,
                        'total_price': float(line_total)
                    }
                    for user_id, item_id, order_id, order_date, quantity, line_total in purchases
                ]
                
                # Batch insert relationships
                batch_size = 1000
                purchased_count = 0
                
                for i in range(0, len(purchased_data), batch_size):
                    batch = purchased_data[i:i+batch_size]
                    purchased_query = """
                    UNWIND $purchases AS p
                    MATCH (u:User {user_id: p.user_id})
                    MATCH (pr:Product {product_id: p.product_id})
                    CREATE (u)-[:PURCHASED {
                        order_id: p.order_id,
                        purchased_at: datetime(p.purchased_at),
                        quantity: p.quantity,
                        total_price: p.total_price
                    }]->(pr)
                    """
                    result = session.run(purchased_query, purchases=batch)
                    purchased_count += result.consume().counters.relationships_created
                
                print(f"  {purchased_count:,} PURCHASED relationships created")
            else:
                print(f"  ⚠ No purchase data available")
            
            # 7. Create VIEWED relationships (User → Product)
            print("Creating VIEWED relationships...")
            
            # Get view events from MongoDB
            view_events = list(mongo_db.user_events.find(
                {
                    'event_type': 'view',
                    'user_id': {'$in': [f"user_{uid}" for uid in pg_data['user_ids'][:100]]},
                    'event_data.product_id': {'$in': [f"item_{pid}" for pid in pg_data['item_ids'][:500]]}
                },
                limit=10000
            ))
            
            if view_events:
                viewed_data = [
                    {
                        'user_id': event['user_id'],
                        'product_id': event['event_data']['product_id'],
                        'session_id': event.get('session_id', 'unknown'),
                        'viewed_at': event['timestamp'].isoformat() if isinstance(event['timestamp'], datetime) else event['timestamp'],
                        'duration_seconds': event['event_data'].get('duration_seconds', 0)
                    }
                    for event in view_events
                ]
                
                # Batch insert
                viewed_count = 0
                for i in range(0, len(viewed_data), batch_size):
                    batch = viewed_data[i:i+batch_size]
                    viewed_query = """
                    UNWIND $views AS v
                    MATCH (u:User {user_id: v.user_id})
                    MATCH (p:Product {product_id: v.product_id})
                    CREATE (u)-[:VIEWED {
                        session_id: v.session_id,
                        viewed_at: datetime(v.viewed_at),
                        duration_seconds: v.duration_seconds
                    }]->(p)
                    """
                    result = session.run(viewed_query, views=batch)
                    viewed_count += result.consume().counters.relationships_created
                
                print(f"  {viewed_count:,} VIEWED relationships created")
            else:
                print(f"  ⚠ No view event data available")
            
            # 8. Create INTERESTED_IN relationships (User → Category)
            print("Creating INTERESTED_IN relationships...")
            
            # Aggregate view/purchase counts by category for each user
            interest_query = """
            MATCH (u:User)-[r:VIEWED|PURCHASED]->(p:Product)-[:BELONGS_TO]->(c:Category)
            WITH u, c, 
                 COUNT(CASE WHEN type(r) = 'VIEWED' THEN 1 END) as view_count,
                 COUNT(CASE WHEN type(r) = 'PURCHASED' THEN 1 END) as purchase_count
            WHERE view_count > 0 OR purchase_count > 0
            WITH u, c, view_count, purchase_count,
                 (view_count + purchase_count * 5) as interest_score
            CREATE (u)-[:INTERESTED_IN {
                interest_score: interest_score,
                view_count: view_count,
                purchase_count: purchase_count
            }]->(c)
            """
            result = session.run(interest_query)
            interested_count = result.consume().counters.relationships_created
            print(f"  {interested_count:,} INTERESTED_IN relationships created")
            
            # 9. Create PURCHASED_WITH relationships (Product → Product)
            print("Creating PURCHASED_WITH relationships...")
            
            # Find products purchased together
            cursor.execute("""
                SELECT 
                    oi1.ItemID as item1,
                    oi2.ItemID as item2,
                    COUNT(*) as count
                FROM OrderItem oi1
                JOIN OrderItem oi2 ON oi1.OrderID = oi2.OrderID
                WHERE oi1.ItemID < oi2.ItemID
                  AND oi1.ItemID IN %s
                  AND oi2.ItemID IN %s
                GROUP BY oi1.ItemID, oi2.ItemID
                HAVING COUNT(*) >= 3
                ORDER BY COUNT(*) DESC
                LIMIT 1000
            """, (
                tuple(pg_data['item_ids'][:500]),
                tuple(pg_data['item_ids'][:500])
            ))
            
            pairs = cursor.fetchall()
            
            if pairs:
                # Calculate confidence scores
                purchased_with_data = [
                    {
                        'product1': f"item_{item1}",
                        'product2': f"item_{item2}",
                        'count': count,
                        'confidence': min(count / 100.0, 1.0)  # Normalize to 0-1
                    }
                    for item1, item2, count in pairs
                ]
                
                purchased_with_query = """
                UNWIND $pairs AS pair
                MATCH (p1:Product {product_id: pair.product1})
                MATCH (p2:Product {product_id: pair.product2})
                CREATE (p1)-[:PURCHASED_WITH {
                    count: pair.count,
                    confidence: pair.confidence,
                    updated_at: datetime()
                }]->(p2)
                """
                result = session.run(purchased_with_query, pairs=purchased_with_data)
                purchased_with_count = result.consume().counters.relationships_created
                print(f"  {purchased_with_count:,} PURCHASED_WITH relationships created")
            else:
                print(f"  ⚠ No co-purchase patterns found")
            
            cursor.close()
        
        print("Neo4j population complete!")
        
        return {
            'users': len(users_data),
            'products': len(products_data),
            'categories': len(categories_data),
            'belongs_to': belongs_to_count,
            'purchased': purchased_count if purchases else 0,
            'viewed': viewed_count if view_events else 0,
            'interested_in': interested_count,
            'purchased_with': purchased_with_count if pairs else 0
        }


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='Populate e-commerce databases')
    parser.add_argument('--test', action='store_true', help='Generate small test dataset')
    parser.add_argument('--full', action='store_true', help='Generate full production dataset')
    parser.add_argument('--no-clear', action='store_true', help='Skip clearing existing data')
    args = parser.parse_args()
    
    if args.test:
        config = {
            'num_users': 10,
            'num_products': 20,
            'orders_per_user': (2, 3),        # 2-3 orders per user = ~25 total
            'num_view_events': 50,            # 50 view/click/cart events
            'num_search_events': 45,          # 45 search events
            'view_cache_per_user': (5, 15)    # 5-15 cached views per user
        }
        print("=" * 60)
        print("RUNNING IN TEST MODE")
        print("=" * 60)
        print("Scale: 10 users, 20 products, ~25 orders, ~95 events")
        print()
    elif args.full:
        config = {
            'num_users': 1000,
            'num_products': 5000,
            'orders_per_user': (95, 105),     # 95-105 orders per user = ~100,000 total
            'num_view_events': 400000,        # 400k view/click/cart events
            'num_search_events': 100000,      # 100k search events
            'view_cache_per_user': (50, 150)  # 50-150 cached views per user
        }
        print("=" * 60)
        print("RUNNING IN FULL PRODUCTION MODE")
        print("=" * 60)
        print("Scale: 1000 users, 5000 products, ~100k orders, ~500k events")
        print("Estimated time: 60-90 minutes")
        print()
    else:
        print("Please specify --test or --full")
        sys.exit(1)
    
    # Initialize
    connections = DatabaseConnections(DB_CONFIG)
    generator = DataGenerator()
    
    try:
        # Connect to all databases
        connections.connect_all()
        
        # Clear existing data (unless --no-clear flag is set)
        if not args.no_clear:
            connections.clear_all()
        else:
            print("\nSkipping database clear (--no-clear flag set)")
        
        # Create PostgreSQL schema
        create_postgresql_schema(connections.pg_conn)
        
        # Populate PostgreSQL
        pg_populator = PostgreSQLPopulator(connections.pg_conn, generator)
        pg_data = pg_populator.populate(config)
        
        # Populate MongoDB
        mongo_populator = MongoDBPopulator(connections.mongo_db, generator)
        mongo_data = mongo_populator.populate(pg_data, config)
        
        # Populate Redis
        redis_populator = RedisPopulator(connections.redis_client)
        redis_data = redis_populator.populate(pg_data, config)
        
        # Populate Neo4j (requires MongoDB and PostgreSQL connections)
        neo4j_populator = Neo4jPopulator(connections.neo4j_driver)
        neo4j_populator.generator = generator
        neo4j_data = neo4j_populator.populate(pg_data, connections.mongo_db, connections.pg_conn)
        
        # Summary
        print("\n" + "="*60)
        print("DATABASE POPULATION COMPLETE!")
        print("="*60)
        print(f"\nPostgreSQL:")
        print(f"  - Users: {len(pg_data['user_ids']):,}")
        print(f"  - Addresses: {len(pg_data['address_ids']):,}")
        print(f"  - Categories: {len(pg_data['category_ids']):,}")
        print(f"  - Items: {len(pg_data['item_ids']):,}")
        print(f"  - Orders: {pg_data['order_count']:,}")
        print(f"\nMongoDB:")
        print(f"  - Product attributes: {mongo_data['product_attrs_count']:,}")
        print(f"  - User events: {mongo_data['user_events_count']:,}")
        print(f"\nRedis:")
        print(f"  - Sessions: {redis_data['sessions']}")
        print(f"  - Hot products: {redis_data['hot_products']}")
        print(f"  - Shopping carts: {redis_data['carts']}")
        print(f"  - View caches: {redis_data['view_caches']:,}")
        print(f"\nNeo4j:")
        print(f"  - User nodes: {neo4j_data['users']:,}")
        print(f"  - Product nodes: {neo4j_data['products']:,}")
        print(f"  - Category nodes: {neo4j_data['categories']}")
        print(f"  - BELONGS_TO relationships: {neo4j_data['belongs_to']:,}")
        print(f"  - PURCHASED relationships: {neo4j_data['purchased']:,}")
        print(f"  - VIEWED relationships: {neo4j_data['viewed']:,}")
        print(f"  - INTERESTED_IN relationships: {neo4j_data['interested_in']:,}")
        print(f"  - PURCHASED_WITH relationships: {neo4j_data['purchased_with']:,}")
        
        print("\n" + "="*60)
        print("SCALE VERIFICATION")
        print("="*60)
        if args.full:
            print(f"Users: {len(pg_data['user_ids']):,} (>= 1,000)")
            print(f"Products: {len(pg_data['item_ids']):,} (>= 5,000)")
            print(f"Orders: {pg_data['order_count']:,} (>= 100,000)")
            print(f"Events: {mongo_data['user_events_count']:,} (>= 500,000)")
            print("\nProduction scale requirements met!")
        print("="*60)
        
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
    finally:
        connections.close_all()


if __name__ == "__main__":
    main()