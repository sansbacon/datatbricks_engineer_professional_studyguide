# Module 2: Delta Lake

## Overview

Delta Lake is an open-source storage layer that runs on top of existing data lakes and provides ACID transactions, scalable metadata handling, and unifies streaming and batch data processing. It's a critical component of the Databricks Lakehouse platform and forms the foundation for reliable, performant data pipelines.

## Key Learning Objectives

- Understand Delta Lake architecture and core features
- Master schema enforcement and schema evolution
- Implement time travel and versioning capabilities
- Apply optimization techniques for performance
- Design scalable data models using Delta Lake
- Implement data quality and governance practices

## 1. Delta Lake Architecture and Core Features

### 1.1 What is Delta Lake?

Delta Lake is built on Apache Parquet and provides:
- **ACID Transactions**: Ensures data consistency and reliability
- **Scalable Metadata Handling**: Efficient metadata operations for large datasets
- **Unified Batch and Streaming**: Single table format for both processing modes
- **Schema Enforcement and Evolution**: Automatic schema validation and safe schema changes
- **Time Travel**: Query historical versions of data
- **Data Versioning**: Complete audit trail of changes

### 1.2 Delta Lake Transaction Log

The Delta Lake transaction log is the single source of truth that tracks all changes made to a Delta table:

```python
# View transaction log information
display(spark.sql("DESCRIBE HISTORY my_delta_table"))

# Access specific version
df = spark.read.format("delta").option("versionAsOf", 2).table("my_delta_table")
```

### 1.3 Table Types: Managed vs External

```sql
-- Managed Delta table (recommended for Unity Catalog)
CREATE TABLE my_managed_table (
    id INT,
    name STRING,
    created_date DATE
) USING DELTA;

-- External/Unmanaged Delta table
CREATE TABLE my_external_table (
    id INT,
    name STRING,
    created_date DATE
) USING DELTA
LOCATION '/mnt/data/my_external_table';
```

## 2. Schema Enforcement and Evolution

### 2.1 Schema Enforcement

Delta Lake automatically validates that data being written matches the table schema:

```python
# This will fail if schema doesn't match
df_wrong_schema.write.format("delta").mode("append").saveAsTable("my_table")

# Enable schema enforcement (default behavior)
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "false")
```

### 2.2 Schema Evolution

Allow schema changes during write operations:

```python
# Enable automatic schema merging
df_new_columns.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("my_table")

# Or set at session level
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
```

### 2.3 Manual Schema Changes

```sql
-- Add new column
ALTER TABLE my_table ADD COLUMN new_column STRING;

-- Change column comment
ALTER TABLE my_table ALTER COLUMN existing_column COMMENT 'Updated description';

-- Drop column (requires Delta Lake 2.3+)
ALTER TABLE my_table DROP COLUMN old_column;
```

## 3. Time Travel and Versioning

### 3.1 Time Travel Queries

Query historical versions of your data:

```sql
-- Query by version number
SELECT * FROM my_table VERSION AS OF 5;

-- Query by timestamp
SELECT * FROM my_table TIMESTAMP AS OF '2023-12-01 10:00:00';
SELECT * FROM my_table TIMESTAMP AS OF current_timestamp() - INTERVAL 1 DAY;
```

```python
# PySpark time travel
df_version = spark.read.format("delta").option("versionAsOf", 5).table("my_table")
df_timestamp = spark.read.format("delta").option("timestampAsOf", "2023-12-01").table("my_table")
```

### 3.2 Version Management

```sql
-- View table history
DESCRIBE HISTORY my_table;

-- Restore table to previous version
RESTORE TABLE my_table TO VERSION AS OF 5;
RESTORE TABLE my_table TO TIMESTAMP AS OF '2023-12-01 10:00:00';
```

## 4. Data Operations and Manipulation

### 4.1 MERGE Operations

Perform upserts (insert/update) efficiently:

```sql
MERGE INTO target_table AS target
USING source_table AS source
ON target.id = source.id
WHEN MATCHED THEN
    UPDATE SET target.name = source.name, target.updated_date = current_timestamp()
WHEN NOT MATCHED THEN
    INSERT (id, name, created_date) VALUES (source.id, source.name, current_timestamp());
```

### 4.2 Change Data Capture (CDC) with APPLY CHANGES

For Lakeflow Declarative Pipelines:

```python
# In a DLT pipeline
import dlt
from pyspark.sql import functions as F

@dlt.table(
    comment="CDC target table"
)
def target_table():
    return (
        dlt.apply_changes(
            target="target_table",
            source="cdc_source",
            keys=["id"],
            sequence_by=col("sequence_num"),
            apply_as_deletes=expr("operation = 'DELETE'"),
            except_column_list=["operation", "sequence_num"]
        )
    )
```

### 4.3 Delete and Update Operations

```sql
-- Delete specific records
DELETE FROM my_table WHERE status = 'inactive';

-- Update records
UPDATE my_table SET status = 'active' WHERE created_date > '2023-01-01';
```

## 5. Performance Optimization

### 5.1 Liquid Clustering (Preferred over Partitioning)

Liquid clustering automatically optimizes data layout:

```sql
-- Create table with liquid clustering
CREATE TABLE clustered_table (
    user_id INT,
    event_date DATE,
    event_type STRING,
    value DOUBLE
) USING DELTA
CLUSTER BY (user_id, event_date);

-- Add clustering to existing table
ALTER TABLE existing_table CLUSTER BY (user_id, event_date);
```

**Benefits over Partitioning:**
- No small file problems
- Automatic optimization
- Better query performance across various access patterns
- No need to specify partition predicates

### 5.2 Deletion Vectors

Efficient deletes without rewriting files:

```sql
-- Enable deletion vectors (default in newer versions)
ALTER TABLE my_table SET TBLPROPERTIES (
    'delta.enableDeletionVectors' = 'true'
);
```

### 5.3 File Compaction and Optimization

```sql
-- Optimize table (compaction)
OPTIMIZE my_table;

-- Z-ORDER optimization for specific columns
OPTIMIZE my_table ZORDER BY (user_id, event_date);

-- Auto-optimize (set at table level)
ALTER TABLE my_table SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
```

### 5.4 Vacuum Operations

Clean up old files:

```sql
-- Remove files older than 7 days (default retention)
VACUUM my_table;

-- Custom retention period
VACUUM my_table RETAIN 24 HOURS;

-- Dry run to see what would be deleted
VACUUM my_table DRY RUN;
```

## 6. Data Quality and Change Data Feed

### 6.1 Change Data Feed (CDF)

Track row-level changes:

```sql
-- Enable CDF on table creation
CREATE TABLE cdf_table (
    id INT,
    name STRING,
    updated_timestamp TIMESTAMP
) USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- Enable CDF on existing table
ALTER TABLE existing_table SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- Query change data
SELECT * FROM table_changes('my_table', 2, 5);
SELECT * FROM table_changes('my_table', '2023-12-01', '2023-12-02');
```

### 6.2 Constraints and Data Quality

```sql
-- Add check constraints
ALTER TABLE my_table ADD CONSTRAINT valid_age CHECK (age >= 0 AND age <= 150);
ALTER TABLE my_table ADD CONSTRAINT valid_email CHECK (email LIKE '%@%.%');

-- NOT NULL constraints
ALTER TABLE my_table ALTER COLUMN id SET NOT NULL;
```

## 7. Advanced Features

### 7.1 Column Mapping

Enable column mapping for schema evolution flexibility:

```sql
ALTER TABLE my_table SET TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name'
);
```

### 7.2 Generated Columns

```sql
CREATE TABLE events (
    event_id STRING,
    event_timestamp TIMESTAMP,
    event_date DATE GENERATED ALWAYS AS (CAST(event_timestamp AS DATE))
) USING DELTA;
```

## 8. Monitoring and Observability

### 8.1 Table Information and Metadata

```sql
-- Detailed table information
DESCRIBE DETAIL my_table;

-- Table properties
SHOW TBLPROPERTIES my_table;

-- Table statistics
ANALYZE TABLE my_table COMPUTE STATISTICS;
```

### 8.2 System Tables for Monitoring

```python
# Query system tables for monitoring
spark.sql("""
    SELECT * FROM system.information_schema.tables 
    WHERE table_schema = 'my_catalog.my_schema'
""").display()
```

## 9. Best Practices

### 9.1 Table Design
- Use liquid clustering instead of partitioning for new tables
- Enable auto-optimize for write-heavy workloads
- Set appropriate retention periods based on compliance requirements
- Use managed tables with Unity Catalog when possible

### 9.2 Performance
- Cluster on columns used in WHERE clauses and JOIN conditions
- Use deletion vectors for tables with frequent deletes
- Regular OPTIMIZE operations for better query performance
- Monitor file sizes and run VACUUM periodically

### 9.3 Data Quality
- Implement check constraints for data validation
- Use CDF for change tracking and downstream processing
- Apply schema evolution carefully in production environments

## 10. Practice Questions

### Question 1
A Delta Lake table was created with the query:
```sql
CREATE TABLE dev.my_table
USING DELTA
LOCATION "/mnt/dev/my_table"
```

After running:
```sql
ALTER TABLE dev.my_table RENAME TO dev.our_table
```

What happens?

**A.** The table name change is recorded in the Delta transaction log  
**B.** The table reference in the metastore is updated and all data files are moved  
**C.** The table reference in the metastore is updated and no data is changed  
**D.** A new Delta transaction log is created for the renamed table  
**E.** All related files and metadata are dropped and recreated in a single ACID transaction  

**Answer: C** - Only the metastore reference is updated; the underlying data and transaction log remain unchanged.

### Question 2
How can you deduplicate data against previously processed records as it's inserted into a Delta table?

**A.** VACUUM the Delta table after each batch completes  
**B.** Rely on Delta Lake schema enforcement to prevent duplicate records  
**C.** Set the configuration delta.deduplicate = true  
**D.** Perform a full outer join on a unique key and overwrite existing data  
**E.** Perform an insert-only merge with a matching condition on a unique key  

**Answer: E** - Use MERGE with WHEN NOT MATCHED to insert only new records.

### Question 3
What approach ensures all tables in the Lakehouse are configured as external, unmanaged Delta Lake tables?

**A.** Whenever a table is being created, make sure that the LOCATION keyword is used  
**B.** When tables are created, make sure that the EXTERNAL keyword is used  
**C.** When the workspace is configured, make sure external cloud storage has been mounted  
**D.** Whenever a database is being created, make sure that the LOCATION keyword is used  
**E.** Use both LOCATION and UNMANAGED keywords  

**Answer: A** - Using LOCATION creates an external table; the data is not managed by Databricks.

## 11. Key Commands Reference

```sql
-- Table Operations
CREATE TABLE table_name USING DELTA LOCATION 'path';
MERGE INTO target USING source ON condition WHEN MATCHED/NOT MATCHED;
OPTIMIZE table_name ZORDER BY (columns);
VACUUM table_name RETAIN 24 HOURS;

-- Schema Management
ALTER TABLE table_name ADD COLUMN new_col STRING;
ALTER TABLE table_name ALTER COLUMN col_name COMMENT 'description';

-- Time Travel
SELECT * FROM table_name VERSION AS OF 5;
SELECT * FROM table_name TIMESTAMP AS OF '2023-01-01';
RESTORE TABLE table_name TO VERSION AS OF 5;

-- Change Data Feed
SELECT * FROM table_changes('table_name', start_version, end_version);

-- Monitoring
DESCRIBE HISTORY table_name;
DESCRIBE DETAIL table_name;
SHOW TBLPROPERTIES table_name;
```

## Summary

Delta Lake provides the foundation for reliable, performant data processing in the Lakehouse architecture. Key exam topics include:

- **Architecture**: Transaction log, ACID properties, file format
- **Schema Management**: Enforcement, evolution, and constraints  
- **Time Travel**: Version queries and table restoration
- **Optimization**: Liquid clustering, deletion vectors, compaction
- **Data Quality**: CDF, constraints, and monitoring
- **Best Practices**: Table design, performance tuning, governance

Understanding these concepts and their practical implementation is crucial for the Databricks Data Engineer Professional certification.