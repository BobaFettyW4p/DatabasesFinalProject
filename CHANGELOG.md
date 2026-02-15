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
