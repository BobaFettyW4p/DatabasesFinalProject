# Database Project Revision Changelog

## [1.0.0] - 2026-02-14

### Added

- First draft of the ERD - all elements are accounted for in a fully relational manner

## [2.0.0] - 2026-02-15

Component: Relational schema restructure
Changes Made:

- Renamed Transaction to Order with proper structure
- Added OrderItem table for one-to-many relationship
- Implemented denormalization in cart/order items (price/name snapshots)
- Added Session table for cross-device tracking
- Enhanced ShoppingCart with status tracking and device info
- Added behavioral tracking tables (RecentlyViewedItem, SearchHistory, UserBehaviorEvent)
- Fixed stock management to track inventory per variation
- Enhanced ReturnItem with all required financial fields
- Added timestamps to all tables
- Fixed circular dependency between Order and OrderTax

Rationale:

- Transaction structure only supported single-item orders
- No session/device tracking
- Missing behavioral data storage
- Denormalization improves read performance for cart/order displays

## [3.0.0] - 2026-02-18

Component: Non-relational schema design

Changes Made:

- Transitioned from a fully relational single-database model to a multi-database model
- Segmented system into dedicated layers:
  - Redis for caching and ephemeral state
  - MongoDB for flexible and high-volume event data
  - Neo4j for graph-based relationships and recommendations
- Moved sessions and shopping carts to Redis for low-latency access
- Migrated behavioral and search data to MongoDB for scalable document storage
- Preserved orders, payments, inventory, and financial data in PostgreSQL to maintain ACID guarantees
- Added Neo4j for recommendation modeling and product relationship queries
- Introduced data synchronization pipelines between storage systems
- Refactored architecture into a layered distributed system (Client -> API -> Data Stores)
Rationale:

Version 1.0 and 2.0 focused on data correctness, normalization, and relational integrity.
However, as the system evolved, the fully relational approach would have limitations in:

- Horizontal scalability
- Real-time personalization performance
- Behavioral analytics efficiency
- Recommendation query complexity
- High-frequency session/cart operations
