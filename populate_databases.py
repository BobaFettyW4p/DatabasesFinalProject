"""
E-Commerce Database Population Script
Populates PostgreSQL, MongoDB, Redis, and Neo4j with synthetic data

Usage:
    python populate_databases.py --test           # Small test dataset (10 users, 20 products)
    python populate_databases.py --full           # Full dataset (1000 users, 5000 products)
    python populate_databases.py --test --no-clear  # Test dataset without clearing existing data

Features:
    - Automatically clears all databases before populating (use --no-clear to skip)
    - Generates realistic e-commerce data with proper relationships
    - Creates orders, payments, shipping options automatically
    - Populates search events, view events, and shopping carts
    - Supports both test and production-scale datasets

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
            print(f"✗ PostgreSQL connection failed: {e}")
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
            print(f"✗ MongoDB connection failed: {e}")
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
            print(f"✗ Redis connection failed: {e}")
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
            print(f"✗ Neo4j connection failed: {e}")
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
                print(f"  ✗ PostgreSQL clear failed: {e}")
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
                print(f"  ✗ MongoDB clear failed: {e}")
        
        # Clear Redis
        if self.redis_client is not None:
            print("\nClearing Redis...")
            try:
                self.redis_client.flushdb()
                print("  Redis cleared")
            except Exception as e:
                print(f"  ✗ Redis clear failed: {e}")
        
        # Clear Neo4j
        if self.neo4j_driver is not None:
            print("\nClearing Neo4j...")
            try:
                with self.neo4j_driver.session() as session:
                    session.run("MATCH (n) DETACH DELETE n")
                print("  Neo4j cleared")
            except Exception as e:
                print(f"  ✗ Neo4j clear failed: {e}")
        
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
        
        # Add User → Address foreign key if it doesn't exist
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
        print(f"✗ Error creating schema: {e}")
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
    
    def populate(self, num_users=10, num_products=20):
        """Populate PostgreSQL with test data"""
        print("\n=== Populating PostgreSQL ===")
        cursor = self.conn.cursor()
        
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
            print(f"  {len(self.address_ids)} addresses inserted")
            
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
            print(f"  {len(self.user_ids)} users inserted")
            
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
                    # parent_category_id was set as idx+1, we need to map it to actual DB ID
                    parent_idx = cat['parent_category_id'] - 1
                    parent_name = list(parent_category_map.keys())[parent_idx] if parent_idx < len(parent_category_map) else None
                    actual_parent_id = parent_category_map.get(parent_name) if parent_name else None
                    
                    cursor.execute(category_query, (
                        cat['name'], cat['description'], actual_parent_id, cat['created_at']
                    ))
                    self.category_ids.append(cursor.fetchone()[0])
            
            print(f"  {len(self.category_ids)} categories inserted")
            
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
            print(f"  {len(self.item_ids)} items inserted")
            
            # 5. Link Items to Categories
            print("Linking items to categories...")
            item_category_query = """
                INSERT INTO ItemCategory (ItemID, CategoryID, IsPrimaryCategory, CreatedAt)
                VALUES (%s, %s, %s, %s)
            """
            
            # Map category types to their database IDs
            # Parent categories are the first 5 in category_ids list
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
            
            print(f"  {len(self.item_ids)} item-category relationships created")
            
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
            print(f"  {len(payment_method_ids)} payment methods created")
            
            # 7. Create sample orders (2-3 per user)
            print("Creating sample orders...")
            
            order_count = 0
            order_item_count = 0
            
            for user_idx, user_id in enumerate(self.user_ids[:num_users]):
                num_orders = random.randint(2, 3)
                
                for _ in range(num_orders):
                    # Order dates in past 60 days
                    days_ago = random.randint(1, 60)
                    order_date = datetime.now() - timedelta(days=days_ago)
                    
                    # Order status
                    status_options = [
                        ('delivered', True, True),   # Has ship and delivery dates
                        ('shipped', True, False),     # Has ship date only
                        ('processing', False, False), # No dates yet
                        ('pending', False, False)     # No dates yet
                    ]
                    
                    status, has_ship, has_delivery = random.choice(status_options)
                    
                    ship_date = order_date + timedelta(days=2) if has_ship else None
                    delivery_date = order_date + timedelta(days=5) if has_delivery else None
                    
                    # Select 1-3 items for order
                    num_items_in_order = random.randint(1, 3)
                    order_items = random.sample(list(zip(self.item_ids, products)), 
                                               min(num_items_in_order, len(self.item_ids)))
                    
                    # Calculate totals
                    subtotal = 0
                    for item_id, product in order_items:
                        quantity = random.randint(1, 2)
                        subtotal += float(product['base_price']) * quantity
                    
                    tax = subtotal * 0.08  # 8% tax
                    shipping_cost = random.choice([5.99, 15.99, 29.99])
                    total = subtotal + tax + shipping_cost
                    
                    # Create order
                    order_number = f"ORD-{fake.uuid4()[:8].upper()}"
                    
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
                        quantity = random.randint(1, 2)
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
            
            print(f"  {order_count} orders created")
            print(f"  {order_item_count} order items created")
            
            # 8. Create sample returns (for delivered orders)
            print("Creating sample returns...")
            
            return_count = 0
            
            # Get delivered orders
            cursor.execute("""
                SELECT o.OrderID, o.OrderDate
                FROM "Order" o
                WHERE o.Status = 'delivered'
                  AND o.UserID IN %s
                LIMIT 10
            """, (tuple(self.user_ids[:num_users]),))
            
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
                
                # Create returns for some delivered orders
                for order_id, order_date in delivered_orders[:5]:  # Only first 5
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
                    # Return requested 5-15 days after delivery
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
            
            print(f"  {return_count} returns created")
            
            # Commit transaction
            self.conn.commit()
            print("PostgreSQL population complete!")
            
            return {
                'user_ids': self.user_ids,
                'address_ids': self.address_ids,
                'category_ids': self.category_ids,
                'item_ids': self.item_ids,
                'products': products  # Return for MongoDB
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
    
    def populate(self, pg_data):
        """Populate MongoDB with test data"""
        print("\n=== Populating MongoDB ===")
        
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
            print(f"  {len(product_attrs)} product attributes inserted")
        
        # 2. Create sample user events (views, clicks, cart additions)
        print("Inserting sample user events...")
        user_events = []
        num_view_events = min(50, len(pg_data['user_ids']) * 5)
        
        for _ in range(num_view_events):
            event = {
                'user_id': f"user_{random.choice(pg_data['user_ids'])}",
                'session_id': f"sess_{fake.uuid4()[:8]}",
                'event_type': random.choice(['view', 'click', 'add_to_cart']),
                'event_data': {
                    'product_id': f"item_{random.choice(pg_data['item_ids'])}",
                    'duration_seconds': random.randint(5, 120)
                },
                'timestamp': fake.date_time_between(start_date='-30d', end_date='now'),
                'device_type': random.choice(['laptop', 'mobile', 'tablet']),
                'created_at': datetime.now()
            }
            user_events.append(event)
        
        # 3. Create search events
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
        
        # Generate searches for each user
        num_search_events = min(45, len(pg_data['user_ids']) * 5)
        
        for _ in range(num_search_events):
            # Select time period based on weights
            time_period = random.choices(
                list(time_weights.keys()),
                weights=[w[2] for w in time_weights.values()]
            )[0]
            
            hour_min, hour_max, _ = time_weights[time_period]
            
            # Create timestamp with specific hour range
            days_ago = random.randint(1, 30)
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
            user_events.append(search_event)
        
        if user_events:
            self.db.user_events.insert_many(user_events)
            view_count = sum(1 for e in user_events if e['event_type'] in ['view', 'click', 'add_to_cart'])
            search_count = sum(1 for e in user_events if e['event_type'] == 'search')
            print(f"  {view_count} user events inserted (views, clicks, cart)")
            print(f"  {search_count} search events inserted")
        
        # 3. Create indexes
        print("Creating indexes...")
        self.db.product_attributes.create_index('product_id', unique=True)
        self.db.product_attributes.create_index('category')
        self.db.user_events.create_index([('user_id', 1), ('timestamp', -1)])
        self.db.user_events.create_index([('event_type', 1), ('timestamp', -1)])
        print("  Indexes created")
        
        return {
            'product_attrs_count': len(product_attrs),
            'user_events_count': len(user_events)
        }


# ============================================================================
# REDIS POPULATOR
# ============================================================================

class RedisPopulator:
    """Populates Redis cache"""
    
    def __init__(self, client):
        self.client = client
    
    def populate(self, pg_data):
        """Populate Redis with test data"""
        print("\n=== Populating Redis ===")
        
        # Clear existing Redis data
        print("Clearing existing Redis keys...")
        # Delete session keys
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
            import json
            for item in cart_items:
                self.client.rpush(f"cart_items:{cart_id}", json.dumps(item))
            self.client.expire(f"cart_items:{cart_id}", 604800)  # 7 days
            
            # Set active cart pointer
            self.client.set(f"active_cart:{user_id}", cart_id, ex=604800)
        
        print(f"  {cart_count} shopping carts created")
        
        return {
            'sessions': session_count,
            'hot_products': hot_product_count,
            'carts': cart_count
        }


# ============================================================================
# NEO4J POPULATOR
# ============================================================================

class Neo4jPopulator:
    """Populates Neo4j graph database"""
    
    def __init__(self, driver):
        self.driver = driver
    
    def populate(self, pg_data):
        """Populate Neo4j with test data"""
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
                for uid in pg_data['user_ids'][:10]  # First 10 users
            ]
            session.run(user_query, users=users_data)
            print(f"  {len(users_data)} User nodes created")
            
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
                for idx, pid in enumerate(pg_data['item_ids'][:20])
            ]
            session.run(product_query, products=products_data)
            print(f"  {len(products_data)} Product nodes created")
            
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
        
        return {
            'users': len(users_data),
            'products': len(products_data),
            'categories': len(categories_data)
        }


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='Populate e-commerce databases')
    parser.add_argument('--test', action='store_true', help='Generate small test dataset')
    parser.add_argument('--full', action='store_true', help='Generate full dataset')
    parser.add_argument('--no-clear', action='store_true', help='Skip clearing existing data')
    args = parser.parse_args()
    
    if args.test:
        num_users = 10
        num_products = 20
        print("Running in TEST mode (10 users, 20 products)")
    elif args.full:
        num_users = 1000
        num_products = 5000
        print("Running in FULL mode (1000 users, 5000 products)")
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
        pg_data = pg_populator.populate(num_users, num_products)
        
        # Populate MongoDB
        mongo_populator = MongoDBPopulator(connections.mongo_db, generator)
        mongo_data = mongo_populator.populate(pg_data)
        
        # Populate Redis
        redis_populator = RedisPopulator(connections.redis_client)
        redis_data = redis_populator.populate(pg_data)
        
        # Populate Neo4j
        neo4j_populator = Neo4jPopulator(connections.neo4j_driver)
        neo4j_populator.generator = generator
        neo4j_data = neo4j_populator.populate(pg_data)
        
        # Summary
        print("\n" + "="*60)
        print("DATABASE POPULATION COMPLETE!")
        print("="*60)
        print(f"PostgreSQL:")
        print(f"  - Users: {len(pg_data['user_ids'])}")
        print(f"  - Addresses: {len(pg_data['address_ids'])}")
        print(f"  - Categories: {len(pg_data['category_ids'])}")
        print(f"  - Items: {len(pg_data['item_ids'])}")
        print(f"\nMongoDB:")
        print(f"  - Product attributes: {mongo_data['product_attrs_count']}")
        print(f"  - User events: {mongo_data['user_events_count']}")
        print(f"\nRedis:")
        print(f"  - Sessions: {redis_data['sessions']}")
        print(f"  - Hot products: {redis_data['hot_products']}")
        print(f"\nNeo4j:")
        print(f"  - User nodes: {neo4j_data['users']}")
        print(f"  - Product nodes: {neo4j_data['products']}")
        print(f"  - Category nodes: {neo4j_data['categories']}")
        print("="*60)
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        connections.close_all()


if __name__ == "__main__":
    main()