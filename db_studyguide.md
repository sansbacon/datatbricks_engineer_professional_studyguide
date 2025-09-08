Exam outline
Section 1: Developing Code for Data Processing using Python and SQL
1.a:Using Python and Tools for development
● Design and implement a scalable Python project structure optimized for Databricks
Asset Bundles (DABs), enabling modular development, deployment automation, and
CI/CD integration.
● Manage and troubleshoot external third-party library installations and dependencies
in Databricks, including PyPI packages, local wheels, and source archives.
● Develop User-Defined Functions (UDFs) using Pandas/Python UDFs
1.b:Building and Testing an ETL pipeline with Lakeflow Declarative Pipelines, SQL, and Apache
Spark on the Databricks platform
● Build and manage reliable, production-ready data pipelines, for batch and streaming
data using Lakeflow Declarative Pipelines and Autoloader
● Create and Automate ETL workloads using Jobs via UI/APIs/CLI
● Explain the advantages and disadvantages of streaming tables compared to
materialized views.
● Use APPLY CHANGES APIs to simplify CDC in Lakeflow Declarative Pipelines
● Compare Spark Structured Streaming and Lakeflow Declarative Pipelines to
determine the optimal approach for building scalable ETL pipelines.
● Create a pipeline component that uses control flow operators (e.g. if/else, foreach,
etc.)
● Choose the appropriate configs for environments and dependencies, high memory
for notebook tasks and auto-optimization to disallow retries
● Develop unit and integration tests using assertDataFrameEqual, assertSchemaEqual,
DataFrame.transform, and testing frameworks, to ensure code correctness including
built-in debugger
Section 2: Data Ingestion & Acquisition:
2.1: Design and implement data ingestion pipelines to efficiently ingest a variety of data
formats including Delta Lake, Parquet, ORC, AVRO, JSON, CSV, XML, Text and Binary from
diverse sources such as message buses and cloud storage.
2.2: Create an append-only data pipeline, capable of handling both batch and streaming
data using Delta
Section 3: Data Transformation, Cleansing and Quality
3.1: Write efficient Spark SQL and PySpark code to apply advanced data transformations,
including window functions, joins, and aggregations, to manipulate and analyze large
Datasets.
3.2: Develop a quarantining process for bad data with Lakeflow Declarative Pipelines or
autoloader in classic jobs
Section 4: Data Sharing and Federation
4.1: Demonstrate delta sharing securely between Databricks deployments using Databricks
to Databricks Sharing(D2D) or to external platforms using open sharing protocol(D2O)
4.2:Configure Lakehouse Federation with proper governance across supported source
Systems.
4.3: Use Delta Share to share live data from Lakehouse to any computing platform, securely
Section 5: Monitoring and Alerting
5.a: Monitoring
● Use system tables for observability over resource utilization, cost, auditing and
workload monitoring.
● Use Query Profiler UI and Spark UI to monitor workloads.
● Use the Databricks REST APIs/Databricks CLI for monitoring jobs and pipelines inc
● Use Lakeflow Declarative Pipelines Event Logs to monitor pipelines
5.b: Alerting
● Use SQL Alerts to monitor data quality
● Use the Workflows UI and Jobs API to set up job status and performance issue
notifications
Section 6:Cost & Performance Optimisation
6.1:Understand how / why using Unity Catalog managed tables reduces operation overhead
and maintenance burden
6.2:Understand delta optimization techniques, such as deletion vectors and liquid
clustering.
6.3:Understand the optimization techniques used by Databricks to ensure performance of
queries on large datasets (data skipping, file pruning, etc)
6.4:Apply Change Data Feed (CDF) to address specific limitations of streaming tables and
enhance latency.
6.5:Use query profile to analyze the query and identify bottlenecks such as bad data
skipping, inefficient types of joins, data shuffling
Section 7: Ensuring Data Security and Compliance
7.a: Applying Data Security mechanisms
● Use ACLs to secure Workspace Objects, enforcing principle of least privilege
including enforcing principles like least privilege, policy enforcement.
● Use row filters and column masks to filter and mask sensitive table data.
● Apply anonymization and pseudonymization methods such as Hashing, Tokenization,
Suppression, and Generalization to confidential data
7.b: Ensuring Compliance
● Implement a compliant batch & streaming pipeline that detects and applies masking
of PII to ensure data privacy.
● Develop a data purging solution ensuring compliance with data retention policies.
Section 8: Data Governance
8.a: Create and add descriptions/metadata about enterprise data to make it more
discoverable
8.b: Demonstrate understanding of Unity Catalog permission inheritance model
Section 9: Debugging and Deploying
9.a: Debugging and Troubleshooting
● Identify pertinent diagnostic information using Spark UI, cluster logs, system tables
and query profiles to troubleshoot errors
● Analyze the errors and remediate the failed job runs with job repairs and parameter
overrides
● Use Lakeflow Declarative Pipelines event logs & the Spark UI to debugLakeflow
Declarative Pipelines and Spark pipelines
9.b: Deploying CI/CD
● Build and Deploy Databricks resources using Databricks Asset Bundles
● Configure and integrate with Git-based CI/CD workflows using Databricks Git
Folders for notebook and code deployment
Section 10: Data Modelling
10.1 Design and implement scalable data models using Delta Lake to manage large
datasets.
10.2: Simplify data layout decisions and optimize query performance using Liquid
Clustering
10.3: Identify the benefits of using liquid Clustering over Partitioning and ZOrder
10.4: Design Dimensional Models for analytical workloads, ensuring efficient querying and
aggregation.
Sample Questions
These questions are retired from a previous version of the exam. The purpose is to show you
objectives as they are stated on the exam guide, and give you a sample question that aligns to the
objective. The exam guide lists the objectives that could be covered on an exam. The best way to
prepare for a certification exam is to review the exam outline in the exam guide.
Question 1
Objective: Identify the results of running a command on a Delta Lake table created with a query
A Delta Lake table was created with the query:
CREATE TABLE dev.my_table
USING DELTA
LOCATION "/mnt/dev/my_table"
Realizing that the table needs to be used by other and its name is misleading, the below code was
executed:
ALTER TABLE dev.my_table RENAME TO dev.our_table
Which result will occur after running the second command?
A. The table name change is recorded in the Delta transaction log.
B. The table reference in the metastore is updated and all data files are moved.
C. The table reference in the metastore is updated and no data is changed.
D. A new Delta transaction log is created for the renamed table.
E. All related files and metadata are dropped and recreated in a single ACID transaction.
Question 2
Objective: Deduplicate data against previously processed records as it is inserted into Delta table.
A data engineer is developing an ETL workflow that could see late-arriving, duplicate records from
its single source. The data engineer knows that they can deduplicate the records within the batch,
but they are looking for another solution.
Which approach allows the data engineer to deduplicate data against previously processed
records as it is inserted into a Delta table?
A. VACUUM the Delta table after each batch completes.
B. Rely on Delta Lake schema enforcement to prevent duplicate records.
C. Set the configuration delta.deduplicate = true.
D. Perform a full outer join on a unique key and overwrite existing data.
E. Perform an insert-only merge with a matching condition on a unique key.
Question 3
Objective: Identify how to configure all tables in the Lakehouse as external, unmanaged Delta Lake
tables
The data architect has mandated that all tables in the Lakehouse should be configured as external,
unmanaged Delta Lake tables.
Which approach will ensure that this requirement is met?
A. Whenever a table is being created, make sure that the LOCATION keyword is used.
B. When tables are created, make sure that the EXTERNAL keyword is used in the CREATE
TABLE statement.
C. When the workspace is being configured, make sure that external cloud object storage has
been mounted.
D. Whenever a database is being created, make sure that the LOCATION keyword is used.
E. Whenever a table is being created, make sure that the LOCATION and UNMANAGED
keywords are used.
Question 4
Objective: Describe permission controls for Databricks jobs
A data engineering team is trying to transfer ownership from its Databricks Workflows away from
an individual that has switched teams. However, they are unsure how permission controls
specifically for Databricks Jobs work.
Which statement correctly describes permission controls for Databricks Jobs?
A. The creator of a Databricks Job will always have "Owner" privileges; this configuration
cannot be changed.
B. Databricks Jobs must have exactly one owner; "Owner" privileges cannot be assigned to a
group.
C. Other than the default "admins" group, only individual users can be granted privileges on
Jobs.
D. Only workspace administrators can grant "Owner" privileges to a group.
E. A user can only transfer Job ownership to a group if they are also a member of that group.
Question 5
Objective: Identify methods for installing python packages…
A data engineer needs to use a Python package to process data. As a result, they need to install
the Python package on all of the nodes in the currently active cluster.
What describes a method of installing a Python package scoped at the notebook level to all nodes
in the currently active cluster?
A. Use %pip install in a notebook cell
B. Use %sh pip install in a notebook cell
C. Run source env/bin/activate in a notebook setup script
D. Install libraries from PyPI using the cluster UI
E. Use b in a notebook cell
Answers
Question 1: C
Question 2: E
Question 3: A
Question 4: B
Question 5: 
