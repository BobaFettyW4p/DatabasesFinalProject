## Project Requirements

- [ ] specify whether each component of the database should be relational or non-relational
- [ ] Draw an ERD for the relational part of the database
- tables
- fields
- types
- relationships
- [ ] consider attributes for each product category
- [ ] identify one part of the system that can be normalized for performance reasons
- explain the tradeoffs
- [ ] create a data flow diagram showing how the relational and non-relational stores exchange data
- describe data freshness and latenct expectations and how we recover from failure
- [ ] Address potential challenges or concerns with the design
- [ ] which data should be stored in-memory? how should we sync with the main databases?
- [ ] Create and describe strategy to track and store user interactions
- [ ] Which queries can a graph database answer more efficiently than a RDBMS?
- create a graph model showing nodes and relationships
- [ ] Create queries to fetch the requested data based on our database design
- [ ] Test the created queries to ensure all run within 2 seconds with 100k entries per collection/table
- if not passed, identify bottlenecks and propose/implement optimizations


## Deliverables
- [ ] relational database ERD showing tables, fields, types, and relationships
- [ ] data flow diagram showing how relational and non-relational stores exchange data
- [ ] list of queries with performance data
- [ ] revision log which summarizes all changes (at least 5 major revisions expected)
- [ ] citation list of generative AI chats used along with brief reflections on lessons learned, agreement, disagreement, and ultimate adaptations made to the AI's work
- [ ] Comprehensive technical report that explains the entire architecture and our decisions made. A good technical report would allow someone else to understand the project and re-create it themselves without the project spec. It will answer these questions:
- which components are relational or non-relational? why did we choose them this way?
- which components are properly normalized and which have been denormalized. Why did we do this?
- how do we maintain data freshness between our relational and non-relational databases. How do we recover from failure?
- What are some potential issues with the design? Why did we choose not to address them?
- what data is stored in memory? how is this data synced with the main database?
- how do we track and store user interactions?
- What are some potential bottlenecks in our system? What optimizations can we perform?

