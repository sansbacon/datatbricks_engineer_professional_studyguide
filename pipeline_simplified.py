# Databricks notebook source
# MAGIC %md ## Claims Trends DLT - Financials
# MAGIC
# MAGIC Simplified pipeline file that imports helper functions from the functions_and_tests notebook.

# COMMAND ----------

# MAGIC %md ## Setup

# COMMAND ----------

dbutils.widgets.text('RAWDIR', '/Volumes/test_catalog/claims_trends/claims_trends_volume/claims_trends_raw_snapshots')

dbutils.widgets.text('START_DATE', '2017')

dbutils.widgets.text('CATALOG', 'test_catalog')

dbutils.widgets.text('SCHEMA', 'claims_trends')

dbutils.widgets.text('MONTH_END', '')

dbutils.widgets.dropdown('RUN_TESTS', 'No', ['No', 'Yes'])

# COMMAND ----------

try:
    RAWDIR = spark.conf.get('claims_trends.raw_snapshots_dir')
except:
    RAWDIR = dbutils.widgets.get('RAWDIR')

try:
    START_DATE = int(spark.conf.get('claims_trends.start_date'))
except:
    START_DATE = int(dbutils.widgets.get('START_DATE'))

try:
    CATALOG = spark.conf.get('claims_trends.catalog')
except:
    CATALOG = dbutils.widgets.get('CATALOG')

try:
    SCHEMA = spark.conf.get('claims_trends.schema')
except:
    SCHEMA = dbutils.widgets.get('SCHEMA')
    
try:
    RUN_TESTS = spark.conf.get('claims_trends.run_tests') == 'Yes'
except:
    RUN_TESTS = dbutils.widgets.get('RUN_TESTS') == 'Yes'

try:
    MONTH_END = spark.conf.get('claims_trends.month_end')
except:
    MONTH_END = dbutils.widgets.get('MONTH_END')

# COMMAND ----------

# mock DLT if unavailable
try:
    import dlt
except ModuleNotFoundError:
    import types

    dlt = types.ModuleType("dlt")

    def expect_or_fail(*args, **kwargs):
        def wrapper(func):
            return func
        return wrapper

    def table(*args, **kwargs):
        def wrapper(func):
            return func
        return wrapper

    def view(*args, **kwargs):
        def wrapper(func):
            return func
        return wrapper

    dlt.expect_or_fail = expect_or_fail
    dlt.table = table
    dlt.view = view

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md ## Import Helper Functions

# COMMAND ----------

# MAGIC %run ./functions_and_tests

# COMMAND ----------

# MAGIC %md ## Ingest New Snapshot Files

# COMMAND ----------

@dlt.table()
@dlt.expect_or_fail('no null dates', 'MONTH_END IS NOT NULL AND QUARTER_END IS NOT NULL')
def msnapshots_raw():
    """Creates msnapshots_raw table by reading monthly snapshot parquet files"""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .load(RAWDIR)
        .transform(add_claim_indicators)
        .join(dlt.read('raw_matters').select('MATTER_SK', 'DATE_REPORTED', 'CLAIMANT_TYPE_SK'), on='MATTER_SK', how='left')
        .withColumnRenamed('AS_OF_DATE', 'MONTH_END')
        .transform(add_quarter_end)
    )

# COMMAND ----------

# MAGIC %md ## Bronze Financials

# COMMAND ----------

# MAGIC %md ### Create Minified AOP and Cause of Loss

# COMMAND ----------

@dlt.table
def msnapshots_bronze_cl01():
    """Bronze layer for open CL01 claims"""
    # handle periods for non-quarter ends
    base = dlt.read('msnapshots_raw')

    base_fix_aop = (
      base
      .join(dlt.read('bronze_aop'), on='AOP_SK', how='left')
      .drop('AOP_SK')
      .join(dlt.read('bronze_aop_minified').drop_duplicates(subset=['AOP_GROUP_2']), on='AOP_GROUP_2', how='left')
      .withColumn('AOP_SK', F.when(F.col('IS_SUPPLEMENTAL') == 1, F.lit(-2)).otherwise(F.col('AOP_SK')))
    )

    # split into LPL and supplemental claims
    lpl = base.where('LOB_SK = 1')
    supp = base.where('LOB_SK > 1')

    lpl_fix_col = (
      lpl
      .join(dlt.read('bronze_lpl_col'), on='PRIMARY_CAUSE_OF_LOSS_SK', how='left')
      .drop('PRIMARY_CAUSE_OF_LOSS_SK')
      .join(dlt.read('bronze_lpl_col_minified').drop_duplicates(subset=['CAUSE_TYPE']), on='CAUSE_TYPE', how='left')
    )

    supp_fix_col = (
      supp
      #.join(dlt.read('bronze_supp_col'), on='PRIMARY_CAUSE_OF_LOSS_SK', how='inner')
      #.drop('PRIMARY_CAUSE_OF_LOSS_SK')
      #.join(dlt.read('bronze_supp_col_minified').drop_duplicates(subset=['CAUSE_TYPE']), on='CAUSE_TYPE', how='inner')
    )

    fix_col = (
      lpl_fix_col.select('MATTER_SK', 'PRIMARY_CAUSE_OF_LOSS_SK')
      .unionByName(supp_fix_col.select('MATTER_SK', 'PRIMARY_CAUSE_OF_LOSS_SK'))
      .distinct()
    )

    base_fix_col = (
      base_fix_aop
      .drop('PRIMARY_CAUSE_OF_LOSS_SK')
      .join(fix_col, on='MATTER_SK', how='left')
    )

    return base_fix_col.drop_duplicates(subset=['MATTER_SK', 'MONTH_END'])

# COMMAND ----------

# MAGIC %md ## Silver Financials

# COMMAND ----------

# MAGIC %md ### Silver Financials Agg

# COMMAND ----------

@dlt.table
@dlt.expect_or_fail('no null dates', 'QUARTER_END IS NOT NULL')
def silver_financials_agg_lpl():
    # step one: aggregate financials at quarterly level

    # combine the quarterly snapshots with the most recent month if not end-of-quarter
    snapshots = dlt.read('msnapshots_bronze_cl01').where('MONTH_END = QUARTER_END')

    if MONTH_END:
        snapshots = snapshots.unionAll(dlt.read('msnapshots_bronze_cl01').where(f"MONTH_END = '{MONTH_END}'"))

    snapshots = snapshots.where('LOB_SK = 1')

    mcols = ['QUARTER_END']
    grpcols = ['PRIMARY_CAUSE_OF_LOSS_SK', 'AOP_SK']
    fincols = ['FIRM_PAID', 'PAID_LOSS', 'INCURRED_LOSS', 'GROUND_UP_PAID']
    selcols = mcols + grpcols + fincols
    ordercols = mcols + grpcols

    return (
        snapshots
        .groupby(mcols + grpcols)
        .agg(
            F.sum('FIRM_PAID').alias('FIRM_PAID'),
            (F.sum('ALAS_PAID')).alias('PAID_LOSS'),
            (F.sum('TOTAL_INCURRED')).alias('INCURRED_LOSS'),
            (F.sum('GROUND_UP_PAID')).alias('GROUND_UP_PAID'),
        )
        .select(selcols)
        .orderBy(ordercols)
        .withColumn('LOB_SK', F.lit(1))
        .withColumn('CLAIMANT_TYPE_SK', F.lit(-2))
    )


@dlt.table
@dlt.expect_or_fail('no null dates', 'QUARTER_END IS NOT NULL')
def silver_financials_agg_supp():
    # step one: aggregate financials at quarterly level
    # combine the quarterly snapshots with the most recent month if not end-of-quarter
    snapshots = dlt.read('msnapshots_bronze_cl01').where('MONTH_END = QUARTER_END')

    if MONTH_END:
        snapshots = snapshots.unionAll(dlt.read('msnapshots_bronze_cl01').where(f"MONTH_END = '{MONTH_END}'"))

    snapshots = snapshots.where('LOB_SK > 1')

    mcols = ['QUARTER_END']
    grpcols = ['PRIMARY_CAUSE_OF_LOSS_SK', 'CLAIMANT_TYPE_SK']
    fincols = ['FIRM_PAID', 'PAID_LOSS', 'INCURRED_LOSS', 'GROUND_UP_PAID']
    selcols = mcols + grpcols + fincols
    ordercols = mcols + grpcols

    return (
        snapshots
        .groupby(mcols + grpcols)
        .agg(
            F.sum('FIRM_PAID').alias('FIRM_PAID'),
            (F.sum('ALAS_PAID')).alias('PAID_LOSS'),
            (F.sum('TOTAL_INCURRED')).alias('INCURRED_LOSS'),
            (F.sum('GROUND_UP_PAID')).alias('GROUND_UP_PAID'),
        )
        .select(selcols)
        .orderBy(ordercols)
        .withColumn('LOB_SK', F.lit(4))
        .withColumn('AOP_SK', F.lit(-2))
    )

# COMMAND ----------

# MAGIC %md ### Silver Financials MOM

# COMMAND ----------

@dlt.table
@dlt.expect_or_fail('no null dates', 'QUARTER_END IS NOT NULL')
def silver_financials_mom_lpl():
    """Calculates the monthly LPL loss emergence as period n+1 totals - period n totals
       Mom function excludes CL01 count as those are not cumulative
       Instead can just join from the aggregate table
    """
    # calculate the monthly value by subtracting snapshots
    mcols = ['QUARTER_END']
    grpcols = ['PRIMARY_CAUSE_OF_LOSS_SK', 'AOP_SK']
    aggcols = ['FIRM_PAID', 'PAID_LOSS', 'INCURRED_LOSS', 'GROUND_UP_PAID']
    tbl = dlt.read('silver_financials_agg_lpl')
    momdf = mom_change(tbl.drop('CL01_COUNT'), mcols, grpcols, aggcols)

    # now return combined dataframe
    return (
        momdf
        .join(tbl.select(*mcols, *grpcols), on=mcols+grpcols, how='left')
        .orderBy(mcols + grpcols)
        .withColumn('LOB_SK', F.lit(1))
        .withColumn('CLAIMANT_TYPE_SK', F.lit(-2))
    )


@dlt.table
@dlt.expect_or_fail('no null dates', 'QUARTER_END IS NOT NULL')
def silver_financials_mom_supp():
    """Calculates the monthly supplemental loss emergence as period n+1 totals - period n totals
       Mom function excludes CL01 count as those are not cumulative
       Instead can just join from the aggregate table
    """
    mcols = ['QUARTER_END']
    grpcols = ['PRIMARY_CAUSE_OF_LOSS_SK', 'CLAIMANT_TYPE_SK']
    aggcols = ['FIRM_PAID', 'PAID_LOSS', 'INCURRED_LOSS', 'GROUND_UP_PAID']
    tbl = dlt.read(f'silver_financials_agg_supp')
    momdf = mom_change(tbl, mcols, grpcols, aggcols)

    # now return combined dataframe
    return (
        momdf
        .join(tbl.select(*mcols, *grpcols), on=mcols+grpcols, how='left')
        .orderBy(mcols + grpcols)
        .withColumn('LOB_SK', F.lit(4))
        .withColumn('AOP_SK', F.lit(-2))
    )

# COMMAND ----------

# MAGIC %md ### Silver CL01 Count

# COMMAND ----------

@dlt.table
def silver_cl01_lpl():
    """Creates silver CL01 count table for LPL"""
    df = dlt.read('msnapshots_bronze_cl01').where('LOB_SK = 1')
    mcols = ['MONTH_END']
    grpcols = ['LOB_SK', 'PRIMARY_CAUSE_OF_LOSS_SK', 'AOP_SK']
    return (
      df
      .groupby(mcols + grpcols)
      .agg(F.sum(F.when((F.col('MATTER_CATEGORY_SK') == 2) & (F.col('MATTER_STATUS_CODE') == 'Open'), 1)
	  .otherwise(0)).alias('CL01_COUNT'))
      .withColumn('LOB_SK', F.lit(1))
      .withColumn('CLAIMANT_TYPE_SK', F.lit(-2))
    )

# COMMAND ----------

@dlt.table
def silver_cl01_supp():
    """Creates silver CL01 count table for supplemental"""
    df = dlt.read('msnapshots_bronze_cl01').where('LOB_SK > 1')
    mcols = ['MONTH_END']
    grpcols = ['PRIMARY_CAUSE_OF_LOSS_SK', 'CLAIMANT_TYPE_SK']
    return (
      df
      .groupby(mcols + grpcols)
      .agg(F.sum(F.when((F.col('MATTER_CATEGORY_SK') == 2) & (F.col('MATTER_STATUS_CODE') == 'Open'), 1)
	  .otherwise(0)).alias('CL01_COUNT'))
      .withColumn('LOB_SK', F.lit(4))
      .withColumn('AOP_SK', F.lit(-2))
    )

# COMMAND ----------

# MAGIC %md ## Gold Tables  
# MAGIC
# MAGIC * msnapshots_agg_loss
# MAGIC * msnapshots_mom_loss

# COMMAND ----------

@dlt.table
@dlt.expect_or_fail('no null dates', 'QUARTER_END IS NOT NULL')
def msnapshots_agg_loss():
    """Combine matter counts and financials into single table"""
    cols = [
        'QUARTER_END',
        'LOB_SK',
        'AOP_SK',
        'PRIMARY_CAUSE_OF_LOSS_SK',
        'CLAIMANT_TYPE_SK',
        'FIRM_PAID',
        'PAID_LOSS',
        'INCURRED_LOSS',
        'GROUND_UP_PAID',
    ]
    lpl = dlt.read('silver_financials_agg_lpl').select(*cols)
    cmb = dlt.read('silver_financials_agg_supp').select(*cols)
    return lpl.unionByName(cmb)

# COMMAND ----------

@dlt.table
@dlt.expect_or_fail('no null dates', 'QUARTER_END IS NOT NULL')
def msnapshots_mom_loss():
    """Combine matter counts mom and financials mom into single table"""
    cols = [
        'QUARTER_END',
        'LOB_SK',
        'AOP_SK',
        'PRIMARY_CAUSE_OF_LOSS_SK',
        'CLAIMANT_TYPE_SK',
        'FIRM_PAID',
        'PAID_LOSS',
        'INCURRED_LOSS',
        'GROUND_UP_PAID',
    ]
    lpl = dlt.read('silver_financials_mom_lpl').select(*cols)
    cmb = dlt.read('silver_financials_mom_supp').select(*cols)
    return lpl.unionByName(cmb)

# COMMAND ----------
