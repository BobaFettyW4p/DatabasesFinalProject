```mermaid
erDiagram

    %% ═══════════════════════════════════════════════════════════
    %% MySQL — Relational Core (16 entities)
    %% ═══════════════════════════════════════════════════════════

    %% ── User & Address ────────────────────────────────────────
    MYSQL_User ||--o{ MYSQL_Order : "places"
    MYSQL_User }o--|| MYSQL_Address : "has primary address"

    %% ── Product Catalog ───────────────────────────────────────
    MYSQL_Item ||--o{ MYSQL_ItemCategory : "belongs to"
    MYSQL_Item ||--o{ MYSQL_ItemAttribute : "has"
    MYSQL_Item ||--o{ MYSQL_ItemVariation : "has variants"
    MYSQL_Item ||--o{ MYSQL_OrderItem : "ordered"
    MYSQL_Item ||--o{ MYSQL_ReturnItem : "returned"
    MYSQL_Category ||--o{ MYSQL_ItemCategory : "contains"
    MYSQL_ItemVariation ||--|| MYSQL_VariationTrait : "has trait"

    %% ── Orders & Fulfillment ──────────────────────────────────
    MYSQL_Order ||--o{ MYSQL_OrderItem : "contains"
    MYSQL_Order }o--|| MYSQL_ShippingOption : "uses"
    MYSQL_Order }o--|| MYSQL_PaymentMethod : "paid with"
    MYSQL_Order }o--|| MYSQL_Address : "ships to"
    MYSQL_Order ||--o{ MYSQL_OrderTax : "has taxes"
    MYSQL_Order ||--o{ MYSQL_ReturnItem : "has returns"
    MYSQL_OrderItem ||--o| MYSQL_ItemVariation : "specific variant"
    MYSQL_OrderTax }o--|| MYSQL_TaxRate : "applies rate"

    %% ── Payment ───────────────────────────────────────────────
    MYSQL_PaymentMethod }o--|| MYSQL_PaymentType : "is type"
    MYSQL_PaymentMethod }o--|| MYSQL_Address : "billing address"

    %% ── Returns ───────────────────────────────────────────────
    MYSQL_ReturnItem }o--|| MYSQL_ShippingOption : "return shipping"

    %% ═══════════════════════════════════════════════════════════
    %% Redis — Session & Cart Cache (3 entities)
    %% ═══════════════════════════════════════════════════════════

    REDIS_Session ||--o{ REDIS_ShoppingCart : "associated with"
    REDIS_ShoppingCart ||--o{ REDIS_ShoppingCartItem : "contains"

    %% ═══════════════════════════════════════════════════════════
    %% Cross-Database References (application-enforced)
    %% ═══════════════════════════════════════════════════════════

    %% Redis → MySQL
    REDIS_Session }o..|| MYSQL_User : "UserID"
    REDIS_ShoppingCart }o..|| MYSQL_User : "UserID"
    REDIS_ShoppingCartItem }o..|| MYSQL_Item : "ItemID"
    REDIS_ShoppingCartItem }o..o| MYSQL_ItemVariation : "ItemVariationID"

    %% MongoDB → MySQL
    MONGO_UserBehaviorEvent }o..|| MYSQL_User : "UserID"
    MONGO_UserBehaviorEvent }o..o| MYSQL_Item : "ItemID"
    MONGO_UserBehaviorEvent }o..o| MYSQL_Category : "CategoryID"
    MONGO_SearchHistory }o..|| MYSQL_User : "UserID"

    %% MongoDB → Redis
    MONGO_UserBehaviorEvent }o..|| REDIS_Session : "SessionID"
    MONGO_SearchHistory }o..o| REDIS_Session : "SessionID"

    %% Neo4j → MySQL
    NEO4J_UserInterest }o..|| MYSQL_User : "(:User) node synced"
    NEO4J_UserInterest }o..|| MYSQL_Category : "(:Category) node synced"
    NEO4J_RecentlyViewedItem }o..|| MYSQL_User : "(:User) node synced"
    NEO4J_RecentlyViewedItem }o..|| MYSQL_Item : "(:Item) node synced"

    %% Neo4j → Redis
    NEO4J_RecentlyViewedItem }o..o| REDIS_Session : "SessionID"

    %% ═══════════════════════════════════════════════════════════
    %% ENTITY DEFINITIONS
    %% ═══════════════════════════════════════════════════════════

    %% ── MySQL Entities ────────────────────────────────────────

    MYSQL_User {
        int UserID PK
        string FirstName
        string LastName
        string Email UK
        string Username UK
        string PasswordHash
        int PrimaryAddressID FK
        datetime CreatedAt
        datetime UpdatedAt
    }

    MYSQL_Address {
        int AddressID PK
        string AddressLine1
        string AddressLine2
        string City
        string StateProvince
        string PostalCode
        string Country
        datetime CreatedAt
    }

    MYSQL_Item {
        int ItemID PK
        string Name
        text Description
        decimal BasePrice
        int TotalStockQuantity
        string ImageURL
        boolean IsActive
        datetime CreatedAt
        datetime UpdatedAt
    }

    MYSQL_Category {
        int CategoryID PK
        string CategoryName UK
        text Description
        int ParentCategoryID FK "nullable for hierarchy"
        datetime CreatedAt
    }

    MYSQL_ItemCategory {
        int ItemCategoryID PK
        int ItemID FK
        int CategoryID FK
        boolean IsPrimaryCategory
        datetime CreatedAt
    }

    MYSQL_ItemAttribute {
        int AttributeID PK
        int ItemID FK
        string AttributeName
        string AttributeValue
        string AttributeUnit "nullable, e.g., hours, grams"
        datetime CreatedAt
    }

    MYSQL_ItemVariation {
        int ItemVariationID PK
        int ItemID FK
        string VariationSKU UK
        int StockQuantity
        decimal PriceAdjustment "nullable, +/- from base price"
        boolean IsAvailable
        datetime CreatedAt
        datetime UpdatedAt
    }

    MYSQL_VariationTrait {
        int VariationTraitID PK
        int ItemVariationID FK
        string TraitType "e.g., Size, Color, Material"
        string TraitValue "e.g., Large, Blue, Cotton"
        datetime CreatedAt
    }

    MYSQL_Order {
        int OrderID PK
        int UserID FK
        string OrderNumber UK
        string Status "pending, processing, shipped, delivered, cancelled"
        decimal SubtotalAmount
        decimal TaxAmount
        decimal ShippingAmount
        decimal TotalAmount
        int ShippingAddressID FK
        int ShippingOptionID FK
        int PaymentMethodID FK
        datetime OrderDate
        datetime ExpectedShipDate
        datetime ActualShipDate "nullable"
        datetime ExpectedDeliveryDate
        datetime ActualDeliveryDate "nullable"
        text OrderNotes "nullable"
        datetime CreatedAt
        datetime UpdatedAt
    }

    MYSQL_OrderItem {
        int OrderItemID PK
        int OrderID FK
        int ItemID FK
        int ItemVariationID FK "nullable"
        int Quantity
        decimal UnitPrice "denormalized: price at purchase"
        decimal LineTotal
        string ItemNameSnapshot "denormalized"
        string VariationDescription "denormalized, e.g., Blue - Large"
        datetime CreatedAt
    }

    MYSQL_ShippingOption {
        int ShippingOptionID PK
        string Name "Standard, Express, Overnight"
        string Description
        string EstimatedDays "e.g., 3-5 business days"
        decimal Rate
        boolean IsActive
        datetime CreatedAt
        datetime UpdatedAt
    }

    MYSQL_TaxRate {
        int TaxRateID PK
        string TaxName "Sales Tax, VAT, etc."
        decimal Rate "percentage"
        string ApplicableRegion "State, Province, Country"
        boolean IsActive
        datetime EffectiveFrom
        datetime EffectiveTo "nullable"
        datetime CreatedAt
    }

    MYSQL_OrderTax {
        int OrderTaxID PK
        int OrderID FK
        int TaxRateID FK
        decimal TaxableAmount
        decimal TaxAmount
        datetime CreatedAt
    }

    MYSQL_PaymentType {
        int PaymentTypeID PK
        string TypeName "CreditCard, BankAccount, PayPal"
        string Description
        boolean IsActive
        datetime CreatedAt
    }

    MYSQL_PaymentMethod {
        int PaymentMethodID PK
        int UserID FK
        int PaymentTypeID FK
        string AccountNumberLast4 "last 4 digits only"
        string CardType "nullable, Visa/MC/Amex"
        string ExpirationMonth "nullable, for cards"
        string ExpirationYear "nullable, for cards"
        string AccountHolderName
        int BillingAddressID FK
        boolean IsDefault
        boolean IsActive
        datetime CreatedAt
        datetime UpdatedAt
    }

    MYSQL_ReturnItem {
        int ReturnID PK
        int OrderID FK
        int OrderItemID FK
        int ItemID FK
        string ReturnReason
        text ReturnNotes "nullable"
        string Status "requested, approved, in_transit, received, refunded, rejected"
        int Quantity
        decimal RefundAmount
        decimal RestockingFee
        string RefundStatus "pending, processed, completed"
        int ReturnShippingOptionID FK "nullable"
        datetime RequestedAt
        datetime ApprovedAt "nullable"
        datetime ReceivedAt "nullable"
        datetime RefundedAt "nullable"
        datetime CreatedAt
        datetime UpdatedAt
    }

    %% ── MongoDB Entities ──────────────────────────────────────

    MONGO_UserBehaviorEvent {
        ObjectId _id PK
        int UserID FK "nullable, refs MySQL User"
        int SessionID FK "refs Redis Session"
        string EventType "view, click, add_to_cart, remove_from_cart, search, filter"
        int ItemID FK "nullable, refs MySQL Item"
        int CategoryID FK "nullable, refs MySQL Category"
        object EventData "JSON for additional context"
        string PageURL
        int DurationSeconds "nullable"
        datetime EventTimestamp
        datetime CreatedAt
    }

    MONGO_SearchHistory {
        ObjectId _id PK
        int UserID FK "nullable, refs MySQL User"
        int SessionID FK "nullable, refs Redis Session"
        string SearchQuery
        int ResultCount
        datetime SearchedAt
        string TimeOfDay "morning, afternoon, evening, night"
        datetime CreatedAt
    }

    %% ── Redis Entities ────────────────────────────────────────

    REDIS_Session {
        int SessionID PK "Redis key: session:{SessionID}"
        string SessionToken UK
        int UserID FK "nullable, refs MySQL User"
        string DeviceType "tablet, laptop, mobile, desktop"
        string IPAddress
        string UserAgent
        datetime CreatedAt
        datetime LastActivityAt
        datetime ExpiresAt "used as Redis TTL basis"
    }

    REDIS_ShoppingCart {
        int ShoppingCartID PK "Redis key: cart:{ShoppingCartID}"
        int UserID FK "nullable, refs MySQL User"
        int SessionID FK "refs Redis Session"
        string Status "active, abandoned, converted"
        decimal TotalAmount
        int TotalItems
        datetime CreatedAt
        datetime UpdatedAt
        datetime AbandonedAt "nullable"
        datetime ConvertedAt "nullable"
    }

    REDIS_ShoppingCartItem {
        int ShoppingCartItemID PK
        int ShoppingCartID FK "refs Redis ShoppingCart"
        int ItemID FK "refs MySQL Item"
        int ItemVariationID FK "nullable, refs MySQL ItemVariation"
        int Quantity
        decimal PriceSnapshot "denormalized: price at time added"
        string ItemNameSnapshot "denormalized: item name"
        datetime AddedAt
        datetime UpdatedAt
    }

    %% ── Neo4j Entities ────────────────────────────────────────
    %% Modeled as graph relationships with properties
    %% (:User)-[:INTERESTED_IN {props}]->(:Category)
    %% (:User)-[:RECENTLY_VIEWED {props}]->(:Item)

    NEO4J_UserInterest {
        int UserInterestID PK "relationship property"
        int UserID FK "(:User) node — synced from MySQL"
        int CategoryID FK "(:Category) node — synced from MySQL"
        int InterestScore "1-10 implicit scoring"
        datetime FirstInterestAt
        datetime LastInterestAt
        datetime CreatedAt
        datetime UpdatedAt
    }

    NEO4J_RecentlyViewedItem {
        int RecentlyViewedItemID PK "relationship property"
        int UserID FK "(:User) node — synced from MySQL"
        int ItemID FK "(:Item) node — synced from MySQL"
        int SessionID FK "nullable, refs Redis Session"
        datetime ViewedAt
        int DurationSeconds "time spent viewing"
        datetime CreatedAt
    }
```

```mermaid
    flowchart LR
    subgraph MYSQL["MySQL"]
        M_User["User"]
        M_Item["Item"]
        M_Category["Category"]
        M_ItemVariation["ItemVariation"]
        M_Order["Order"]
        M_Address["Address"]
        M_OrderItem["OrderItem"]
        M_OrderTax["OrderTax"]
        M_TaxRate["TaxRate"]
        M_PaymentMethod["PaymentMethod"]
        M_PaymentType["PaymentType"]
        M_ShippingOption["ShippingOption"]
        M_ReturnItem["ReturnItem"]
        M_ItemCategory["ItemCategory"]
        M_ItemAttribute["ItemAttribute"]
        M_ItemVariation2["ItemVariation"]
        M_VariationTrait["VariationTrait"]
    end

    subgraph MONGODB["MongoDB"]
        MG_Behavior["UserBehaviorEvent"]
        MG_Search["SearchHistory"]
    end

    subgraph REDIS["Redis"]
        R_Session["Session"]
        R_Cart["ShoppingCart"]
        R_CartItem["ShoppingCartItem"]
    end

    subgraph NEO4J["Neo4j"]
        N_Interest["[:INTERESTED_IN]"]
        N_Viewed["[:RECENTLY_VIEWED]"]
    end

    %% Redis → MySQL
    R_Session -. "UserID" .-> M_User
    R_Cart -. "UserID" .-> M_User
    R_CartItem -. "ItemID" .-> M_Item
    R_CartItem -. "ItemVariationID" .-> M_ItemVariation

    %% MongoDB → MySQL & Redis
    MG_Behavior -. "UserID" .-> M_User
    MG_Behavior -. "ItemID" .-> M_Item
    MG_Behavior -. "CategoryID" .-> M_Category
    MG_Behavior -. "SessionID" .-> R_Session
    MG_Search -. "UserID" .-> M_User
    MG_Search -. "SessionID" .-> R_Session

    %% Neo4j → MySQL & Redis
    N_Interest -. "(:User)" .-> M_User
    N_Interest -. "(:Category)" .-> M_Category
    N_Viewed -. "(:User)" .-> M_User
    N_Viewed -. "(:Item)" .-> M_Item
    N_Viewed -. "SessionID" .-> R_Session
```
