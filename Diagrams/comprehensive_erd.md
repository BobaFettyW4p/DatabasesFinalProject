```mermaid
erDiagram

    %% ═══════════════════════════════════════════════════════════
    %% PostgreSQL — Relational Core (16 entities)
    %% ═══════════════════════════════════════════════════════════

    %% ── User & Address ────────────────────────────────────────
    PostgreSQL_User ||--o{ PostgreSQL_Order : "places"
    PostgreSQL_User }o--|| PostgreSQL_Address : "has primary address"

    %% ── Product Catalog ───────────────────────────────────────
    PostgreSQL_Item ||--o{ PostgreSQL_ItemCategory : "belongs to"
    PostgreSQL_Item ||--o{ PostgreSQL_ItemAttribute : "has"
    PostgreSQL_Item ||--o{ PostgreSQL_ItemVariation : "has variants"
    PostgreSQL_Item ||--o{ PostgreSQL_OrderItem : "ordered"
    PostgreSQL_Item ||--o{ PostgreSQL_ReturnItem : "returned"
    PostgreSQL_Category ||--o{ PostgreSQL_ItemCategory : "contains"
    PostgreSQL_ItemVariation ||--|| PostgreSQL_VariationTrait : "has trait"

    %% ── Orders & Fulfillment ──────────────────────────────────
    PostgreSQL_Order ||--o{ PostgreSQL_OrderItem : "contains"
    PostgreSQL_Order }o--|| PostgreSQL_ShippingOption : "uses"
    PostgreSQL_Order }o--|| PostgreSQL_PaymentMethod : "paid with"
    PostgreSQL_Order }o--|| PostgreSQL_Address : "ships to"
    PostgreSQL_Order ||--o{ PostgreSQL_OrderTax : "has taxes"
    PostgreSQL_Order ||--o{ PostgreSQL_ReturnItem : "has returns"
    PostgreSQL_OrderItem ||--o| PostgreSQL_ItemVariation : "specific variant"
    PostgreSQL_OrderTax }o--|| PostgreSQL_TaxRate : "applies rate"

    %% ── Payment ───────────────────────────────────────────────
    PostgreSQL_PaymentMethod }o--|| PostgreSQL_PaymentType : "is type"
    PostgreSQL_PaymentMethod }o--|| PostgreSQL_Address : "billing address"

    %% ── Returns ───────────────────────────────────────────────
    PostgreSQL_ReturnItem }o--|| PostgreSQL_ShippingOption : "return shipping"

    %% ═══════════════════════════════════════════════════════════
    %% Redis — Session & Cart Cache (3 entities)
    %% ═══════════════════════════════════════════════════════════

    REDIS_Session ||--o{ REDIS_ShoppingCart : "associated with"
    REDIS_ShoppingCart ||--o{ REDIS_ShoppingCartItem : "contains"

    %% ═══════════════════════════════════════════════════════════
    %% Cross-Database References (application-enforced)
    %% ═══════════════════════════════════════════════════════════

    %% Redis → PostgreSQL
    REDIS_Session }o..|| PostgreSQL_User : "UserID"
    REDIS_ShoppingCart }o..|| PostgreSQL_User : "UserID"
    REDIS_ShoppingCartItem }o..|| PostgreSQL_Item : "ItemID"
    REDIS_ShoppingCartItem }o..o| PostgreSQL_ItemVariation : "ItemVariationID"

    %% MongoDB → PostgreSQL
    MONGO_UserBehaviorEvent }o..|| PostgreSQL_User : "UserID"
    MONGO_UserBehaviorEvent }o..o| PostgreSQL_Item : "ItemID"
    MONGO_UserBehaviorEvent }o..o| PostgreSQL_Category : "CategoryID"
    MONGO_SearchHistory }o..|| PostgreSQL_User : "UserID"

    %% MongoDB → Redis
    MONGO_UserBehaviorEvent }o..|| REDIS_Session : "SessionID"
    MONGO_SearchHistory }o..o| REDIS_Session : "SessionID"

    %% Neo4j → PostgreSQL
    NEO4J_UserInterest }o..|| PostgreSQL_User : "(:User) node synced"
    NEO4J_UserInterest }o..|| PostgreSQL_Category : "(:Category) node synced"
    NEO4J_RecentlyViewedItem }o..|| PostgreSQL_User : "(:User) node synced"
    NEO4J_RecentlyViewedItem }o..|| PostgreSQL_Item : "(:Item) node synced"

    %% Neo4j → Redis
    NEO4J_RecentlyViewedItem }o..o| REDIS_Session : "SessionID"

    %% ═══════════════════════════════════════════════════════════
    %% ENTITY DEFINITIONS
    %% ═══════════════════════════════════════════════════════════

    %% ── PostgreSQL Entities ────────────────────────────────────────

    PostgreSQL_User {
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

    PostgreSQL_Address {
        int AddressID PK
        string AddressLine1
        string AddressLine2
        string City
        string StateProvince
        string PostalCode
        string Country
        datetime CreatedAt
    }

    PostgreSQL_Item {
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

    PostgreSQL_Category {
        int CategoryID PK
        string CategoryName UK
        text Description
        int ParentCategoryID FK "nullable for hierarchy"
        datetime CreatedAt
    }

    PostgreSQL_ItemCategory {
        int ItemCategoryID PK
        int ItemID FK
        int CategoryID FK
        boolean IsPrimaryCategory
        datetime CreatedAt
    }

    PostgreSQL_ItemAttribute {
        int AttributeID PK
        int ItemID FK
        string AttributeName
        string AttributeValue
        string AttributeUnit "nullable, e.g., hours, grams"
        datetime CreatedAt
    }

    PostgreSQL_ItemVariation {
        int ItemVariationID PK
        int ItemID FK
        string VariationSKU UK
        int StockQuantity
        decimal PriceAdjustment "nullable, +/- from base price"
        boolean IsAvailable
        datetime CreatedAt
        datetime UpdatedAt
    }

    PostgreSQL_VariationTrait {
        int VariationTraitID PK
        int ItemVariationID FK
        string TraitType "e.g., Size, Color, Material"
        string TraitValue "e.g., Large, Blue, Cotton"
        datetime CreatedAt
    }

    PostgreSQL_Order {
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

    PostgreSQL_OrderItem {
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

    PostgreSQL_ShippingOption {
        int ShippingOptionID PK
        string Name "Standard, Express, Overnight"
        string Description
        string EstimatedDays "e.g., 3-5 business days"
        decimal Rate
        boolean IsActive
        datetime CreatedAt
        datetime UpdatedAt
    }

    PostgreSQL_TaxRate {
        int TaxRateID PK
        string TaxName "Sales Tax, VAT, etc."
        decimal Rate "percentage"
        string ApplicableRegion "State, Province, Country"
        boolean IsActive
        datetime EffectiveFrom
        datetime EffectiveTo "nullable"
        datetime CreatedAt
    }

    PostgreSQL_OrderTax {
        int OrderTaxID PK
        int OrderID FK
        int TaxRateID FK
        decimal TaxableAmount
        decimal TaxAmount
        datetime CreatedAt
    }

    PostgreSQL_PaymentType {
        int PaymentTypeID PK
        string TypeName "CreditCard, BankAccount, PayPal"
        string Description
        boolean IsActive
        datetime CreatedAt
    }

    PostgreSQL_PaymentMethod {
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

    PostgreSQL_ReturnItem {
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
        int UserID FK "nullable, refs PostgreSQL User"
        int SessionID FK "refs Redis Session"
        string EventType "view, click, add_to_cart, remove_from_cart, search, filter"
        int ItemID FK "nullable, refs PostgreSQL Item"
        int CategoryID FK "nullable, refs PostgreSQL Category"
        object EventData "JSON for additional context"
        string PageURL
        int DurationSeconds "nullable"
        datetime EventTimestamp
        datetime CreatedAt
    }

    MONGO_SearchHistory {
        ObjectId _id PK
        int UserID FK "nullable, refs PostgreSQL User"
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
        int UserID FK "nullable, refs PostgreSQL User"
        string DeviceType "tablet, laptop, mobile, desktop"
        string IPAddress
        string UserAgent
        datetime CreatedAt
        datetime LastActivityAt
        datetime ExpiresAt "used as Redis TTL basis"
    }

    REDIS_ShoppingCart {
        int ShoppingCartID PK "Redis key: cart:{ShoppingCartID}"
        int UserID FK "nullable, refs PostgreSQL User"
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
        int ItemID FK "refs PostgreSQL Item"
        int ItemVariationID FK "nullable, refs PostgreSQL ItemVariation"
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
        int UserID FK "(:User) node — synced from PostgreSQL"
        int CategoryID FK "(:Category) node — synced from PostgreSQL"
        int InterestScore "1-10 implicit scoring"
        datetime FirstInterestAt
        datetime LastInterestAt
        datetime CreatedAt
        datetime UpdatedAt
    }

    NEO4J_RecentlyViewedItem {
        int RecentlyViewedItemID PK "relationship property"
        int UserID FK "(:User) node — synced from PostgreSQL"
        int ItemID FK "(:Item) node — synced from PostgreSQL"
        int SessionID FK "nullable, refs Redis Session"
        datetime ViewedAt
        int DurationSeconds "time spent viewing"
        datetime CreatedAt
    }
```

```mermaid
    flowchart LR
    subgraph PostgreSQL["PostgreSQL"]
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

    %% Redis → PostgreSQL
    R_Session -. "UserID" .-> M_User
    R_Cart -. "UserID" .-> M_User
    R_CartItem -. "ItemID" .-> M_Item
    R_CartItem -. "ItemVariationID" .-> M_ItemVariation

    %% MongoDB → PostgreSQL & Redis
    MG_Behavior -. "UserID" .-> M_User
    MG_Behavior -. "ItemID" .-> M_Item
    MG_Behavior -. "CategoryID" .-> M_Category
    MG_Behavior -. "SessionID" .-> R_Session
    MG_Search -. "UserID" .-> M_User
    MG_Search -. "SessionID" .-> R_Session

    %% Neo4j → PostgreSQL & Redis
    N_Interest -. "(:User)" .-> M_User
    N_Interest -. "(:Category)" .-> M_Category
    N_Viewed -. "(:User)" .-> M_User
    N_Viewed -. "(:Item)" .-> M_Item
    N_Viewed -. "SessionID" .-> R_Session
```
