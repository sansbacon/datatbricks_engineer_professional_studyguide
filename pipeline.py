 Databricks notebook source
# MAGIC %md ## Claims Trends DLT - Financials

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

import calendar
import datetime

import pandas as pd
import pyspark.pandas as ps
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

def add_quarter_end(df, colname='MONTH_END'):
    """"Adds quarter end column to dataframe"""
    return (
        df
        .withColumn(
            'QUARTER_END',
            F.when(F.last_day(F.col(colname)) == F.last_day(F.trunc(F.col(colname), 'quarter') + F.expr("INTERVAL 2 MONTH")), F.col(colname))
           .otherwise(F.last_day(F.trunc(F.col(colname), 'quarter') + F.expr("INTERVAL 2 MONTH")))
        )
    )


def test_add_quarter_end():
    """Tests add quarter end"""
    s = datetime.date(2024, 1, 31)
    e = datetime.date(2024, 12, 31)

    # Create a DataFrame with month-end dates
    dates_df = spark.sql(f"""
      SELECT explode(sequence(to_date('{s}'), to_date('{e}'), interval 1 month)) as d"""
    )

    df = (
        dates_df.select(F.last_day(F.col("d")).alias("MONTH_END"))
        .transform(add_quarter_end)
    )

    qe = [row[0] for row in df.select('QUARTER_END').collect()]
    periods = [(3, 31)] * 3 + [(6, 30)] * 3 + [(9, 30)] * 3 + [(12, 31)] * 3
    expected = [datetime.date(2024, m, d) for m, d in periods]
    msg = f'Wrong quarter ends {qe=}'
    assert qe == expected, f"ERROR: {msg}"


if RUN_TESTS:
    test_add_quarter_end()

# COMMAND ----------

def aggregate_financials(df, grpcols, mcols=None, fincols=None):
    # setup dataframes based on LOB
    # Create the aggregated files
    mcols = mcols or ['MONTH_END', 'QUARTER_END']
    fincols = fincols or ['FIRM_PAID', 'PAID_LOSS', 'INCURRED_LOSS', 'GROUND_UP_PAID', 'CL01_COUNT']
    selcols = mcols + grpcols + fincols
    ordercols = mcols + grpcols

    return (
        df
        .groupby(mcols + grpcols)
        .agg(
            F.sum(F.when((F.col('MATTER_CATEGORY_SK') == 2) & (F.col('MATTER_STATUS_CODE') == 'Open'), 1)
                  .otherwise(0)).alias('CL01_COUNT'),
            F.sum('FIRM_PAID').alias('FIRM_PAID'),
            (F.sum('ALAS_PAID')).alias('PAID_LOSS'),
            (F.sum('TOTAL_INCURRED')).alias('INCURRED_LOSS'),
            (F.sum('GROUND_UP_PAID')).alias('GROUND_UP_PAID'),
        )
        .select(selcols)
        .orderBy(ordercols)
    )


def test_aggregate_financials():
    """Tests the aggregate financials function"""
    # Input data as dictionaries
    input_data = [
        {
            "MONTH_END": datetime.date(2024, 1, 31),
            "QUARTER_END": datetime.date(2024, 3, 31),
            "MATTER_CATEGORY_SK": 2,
            "MATTER_STATUS_CODE": "Open",
            "FIRM_PAID": 1000.0,
            "ALAS_PAID": 500.0,
            "COINS_PAID": 300.0,
            "TOTAL_INCURRED": 1200.0,
            "COINS_RESERVE": 200.0,
            "GROUND_UP_PAID": 800.0,
            "LOB": 1,
        },
        {
            "MONTH_END": datetime.date(2024, 1, 31),
            "QUARTER_END": datetime.date(2024, 3, 31),
            "MATTER_CATEGORY_SK": 2,
            "MATTER_STATUS_CODE": "Closed",
            "FIRM_PAID": 2000.0,
            "ALAS_PAID": 700.0,
            "COINS_PAID": 400.0,
            "TOTAL_INCURRED": 1500.0,
            "COINS_RESERVE": 300.0,
            "GROUND_UP_PAID": 900.0,
            "LOB": 1,
        },
        {
            "MONTH_END": datetime.date(2024, 2, 28),
            "QUARTER_END": datetime.date(2024, 3, 31),
            "MATTER_CATEGORY_SK": 2,
            "MATTER_STATUS_CODE": "Open",
            "FIRM_PAID": 1500.0,
            "ALAS_PAID": 600.0,
            "COINS_PAID": 300.0,
            "TOTAL_INCURRED": 1300.0,
            "COINS_RESERVE": 300.0,
            "GROUND_UP_PAID": 1000.0,
            "LOB": 4,
        },
    ]

    input_df = spark.createDataFrame(pd.DataFrame(input_data))

    # Expected data as dictionaries
    expected_data = [
        {
            "MONTH_END": datetime.date(2024, 1, 31),
            "QUARTER_END": datetime.date(2024, 3, 31),
            "LOB": 1,
            "CL01_COUNT": 1,
            "FIRM_PAID": 3000.0,
            "PAID_LOSS": 1900.0,
            "INCURRED_LOSS": 3900.0,
            "GROUND_UP_PAID": 2400.0,
        },
        {
            "MONTH_END": datetime.date(2024, 2, 28),
            "QUARTER_END": datetime.date(2024, 3, 31),
            "LOB": 4,
            "CL01_COUNT": 1,
            "FIRM_PAID": 1500.0,
            "PAID_LOSS": 900.0,
            "INCURRED_LOSS": 1900.0,
            "GROUND_UP_PAID": 1300.0,
        },
    ]

    expected_df = spark.createDataFrame(pd.DataFrame(expected_data))

    # Run the function
    grpcols = ["LOB"]
    result_df = aggregate_financials(input_df, grpcols)

    # Assert the results
    if not result_df.collect() == expected_df.select(*result_df.columns).collect():
        display(result_df)
        display(expected_df)
        raise ValueError("ERROR: Incorrect results")

    else:
        print('Test passed')


if RUN_TESTS:
    test_aggregate_financials()

# COMMAND ----------

def add_claim_indicators(df):
    """Adds claim indicators to a dataframe"""
    added = (
      df
      .withColumn('IS_CLAIM', F.when(F.col('MATTER_CATEGORY_SK') < 9, 1).otherwise(0))
      .withColumn(
            'IS_REAL_CLAIM',
            F.when(
                F.col('MATTER_CATEGORY_SK').isin([2, 4, 5, 6, 7]), 1
            ).otherwise(0)
      )
    )

    if 'LOB_SK' in df.columns:
      return (
        added
        .withColumn('IS_SUPPLEMENTAL', F.when(F.col('LOB_SK') == 1, 0).otherwise(1))
    )

    return added.withColumn('IS_SUPPLEMENTAL', F.when(F.col('LOB') == 'LPL', 0).otherwise(1))


def test_add_claim_indicators():
    # Input data as dictionary
    input_data = {
        "MATTER_CATEGORY_SK": [2, 4, 9, 10],
        "LOB_SK": [1, 2, 1, 3],
    }

    # Expected data as dictionary
    expected_data = {
        "MATTER_CATEGORY_SK": [2, 4, 9, 10],
        "LOB_SK": [1, 2, 1, 3],
        "IS_CLAIM": [1, 1, 0, 0],
        "IS_REAL_CLAIM": [1, 1, 0, 0],
        "IS_SUPPLEMENTAL": [0, 1, 0, 1],
    }

    # Create input and expected PySpark DataFrames
    input_df = spark.createDataFrame(pd.DataFrame(input_data))
    expected_df = spark.createDataFrame(pd.DataFrame(expected_data))

    # Run the function
    result_df = add_claim_indicators(input_df)

    # Collect the results for comparison
    result_data = result_df.orderBy("MATTER_CATEGORY_SK", "LOB_SK").collect()
    expected_data = expected_df.orderBy("MATTER_CATEGORY_SK", "LOB_SK").collect()

    # Assert the results
    if not result_data == expected_data:
        display(result_df)
        display(expected_df)
        raise ValueError("Test failed: Results do not match expected output.")

    print("Test passed!")


if RUN_TESTS:
    test_add_claim_indicators()

# COMMAND ----------

def get_snapshot_end_year_month():
    """Returns the year and month of the snapshot"""
    today = datetime.date.today()

    if today.month == 1:
        return (today.year - 1, 12)
    return (today.year, today.month - 1)


def get_snapshot_periods(start_date, year, month):
    # Define start and end dates
    s = datetime.date(start_date, 1, 31)
    last_day = calendar.monthrange(year, month)[1]
    e = datetime.date(year, month, last_day)

    # Create a DataFrame with month-end dates
    dates_df = spark.sql(f"""
      SELECT explode(sequence(to_date('{s}'), to_date('{e}'), interval 1 month)) as d"""
    )

    return dates_df.select(F.last_day(F.col("d")).alias("MONTH_END"))


def test_get_snapshot_periods():
    """Tests get snapshot periods"""
    start_date = 2017
    year, month = (2024, 12)
    dates_df = get_snapshot_periods(start_date, year, month)

    # test minimum date
    min_date = dates_df.orderBy('MONTH_END').first()[0]
    assert min_date == datetime.date(start_date, 1, 31), f'Minimum date should be 1/1/{start_date}'

    # test maximum date
    max_date = dates_df.orderBy(F.desc('MONTH_END')).first()[0]
    assert max_date == datetime.date(year, month, 31), f'Maximum date should be {month}/31/{year}'
    print('Test passed')


if RUN_TESTS:
    test_get_snapshot_periods()

# COMMAND ----------

def mom_change(df, mcols, grpcols, aggcols):
    """Calculates the month-over-month change in a dataframe"""
    if grpcols:
        window_spec = Window.partitionBy(grpcols).orderBy(mcols)
    else:
        window_spec = Window.orderBy(mcols)
        grpcols = []

    # Add lagged columns for aggcols
    for aggcol in aggcols:
        df = (
          df
          .withColumn(f"{aggcol}_PREV", F.lag(aggcol).over(window_spec))
          .withColumn(f"{aggcol}_DIFF", F.col(aggcol) - F.col(f"{aggcol}_PREV"))
          .drop(aggcol, f"{aggcol}_PREV")
          .withColumnRenamed(f"{aggcol}_DIFF", aggcol)
        )

    # return mom dataframe
    return df


def test_mom_change():
    """Tests the mom_change function"""
    # Predefined date variables
    d1 = datetime.date(2024, 1, 31)
    d2 = datetime.date(2024, 2, 28)
    d3 = datetime.date(2024, 3, 31)

    # Input data as dictionary
    input_data = {
        "MONTH_END": [d1, d2, d3],
        "LOB": [1, 1, 1],
        "MATTER_COUNT": [5, 8, 12],
        "CLAIM_COUNT": [10, 18, 25],
    }

    # Expected data as dictionary
    expected_data = {
        "MONTH_END": [d1, d2, d3],
        "LOB": [1, 1, 1],
        "MATTER_COUNT": [None, 3, 4],
        "CLAIM_COUNT": [None, 8, 7],
    }

    # Create input and expected PySpark DataFrames
    input_df = spark.createDataFrame(pd.DataFrame(input_data))
    expected_df = spark.createDataFrame(pd.DataFrame(expected_data))

    # Run the function
    mcols = "MONTH_END"
    grpcols = ["LOB"]
    aggcols = ["MATTER_COUNT", "CLAIM_COUNT"]
    result_df = mom_change(input_df, mcols, grpcols, aggcols)

    # Collect the results for comparison
    result_data = result_df.orderBy("MONTH_END", "LOB").collect()
    expected_data = expected_df.orderBy("MONTH_END", "LOB").collect()

    # Assert the results
    if not result_data == expected_data:
        display(result_df)
        display(expected_df)
        raise ValueError("Test failed: Results do not match expected output.")

    print("Test passed!")


if RUN_TESTS:
    test_mom_change()

# COMMAND ----------

def is_previous_month_end_quarter_end(reference_date=None):
    if reference_date is None:
        reference_date = datetime.date.today()
    
    # First day of the current month
    first_of_month = reference_date.replace(day=1)
    # Last day of the previous month
    last_day_prev_month = first_of_month - datetime.timedelta(days=1)
    
    # Check if it's a quarter end
    is_quarter_end = (
        last_day_prev_month.month in [3, 6, 9, 12] and
        last_day_prev_month.day == last_day_prev_month.replace(day=1).replace(
            month=last_day_prev_month.month % 12 + 1 if last_day_prev_month.month != 12 else 1,
            year=last_day_prev_month.year + (1 if last_day_prev_month.month == 12 else 0)
        ) - datetime.timedelta(days=1)
    ).day

    return int(is_quarter_end)


@F.pandas_udf('integer')
def is_previous_month_end_quarter_end_udf(s: pd.Series) -> pd.Series:
    return s.apply(is_previous_month_end_quarter_end)


def test_is_previous_month_end_quarter_end():
    test_cases = [
        # Format: (reference_date, expected_last_day, expected_result)
        (datetime.date(2025, 4, 1), datetime.date(2025, 3, 31), True),   # Q1 end
        (datetime.date(2025, 7, 1), datetime.date(2025, 6, 30), True),   # Q2 end
        (datetime.date(2025, 10, 1), datetime.date(2025, 9, 30), True),  # Q3 end
        (datetime.date(2026, 1, 1), datetime.date(2025, 12, 31), True),  # Q4 end
        (datetime.date(2025, 5, 1), datetime.date(2025, 4, 30), False),  # Not quarter end
        (datetime.date(2025, 8, 1), datetime.date(2025, 7, 31), False),  # Not quarter end
    ]

    for ref_date, expected_last_day, expected_result in test_cases:
        actual_last_day, actual_result = is_previous_month_end_quarter_end(ref_date)
        assert actual_last_day == expected_last_day, f"Failed date check for {ref_date}"
        assert actual_result == expected_result, f"Failed quarter check for {ref_date}"
    
    print("All tests passed.")


if RUN_TESTS:
    test_is_previous_month_end_quarter_end()   

# COMMAND ----------

def get_filtered_snapshots(snapshots_df, month_end=None):
    """
    Filters snapshots to include quarter-end snapshots and optionally a specific month-end.
    
    Args:
        snapshots_df: DataFrame with snapshots
        month_end: Optional specific month-end date to include (string format: 'YYYY-MM-DD')
    
    Returns:
        Filtered DataFrame
    """
    result = snapshots_df.where('MONTH_END = QUARTER_END')
    
    if month_end:
        result = result.unionAll(snapshots_df.where(f"MONTH_END = '{month_end}'"))
    
    return result


def aggregate_financials_by_lob(snapshots_df, lob_filter, grpcols, month_end=None):
    """
    Aggregates financial data for a specific LOB (Line of Business).
    
    Args:
        snapshots_df: Input DataFrame with snapshot data
        lob_filter: SQL WHERE clause to filter LOB (e.g., 'LOB_SK = 1' or 'LOB_SK > 1')
        grpcols: List of grouping columns (e.g., ['PRIMARY_CAUSE_OF_LOSS_SK', 'AOP_SK'])
        month_end: Optional specific month-end date to include
    
    Returns:
        Aggregated DataFrame with financial metrics
    """
    # Filter snapshots
    filtered = get_filtered_snapshots(snapshots_df, month_end).where(lob_filter)
    
    mcols = ['QUARTER_END']
    fincols = ['FIRM_PAID', 'PAID_LOSS', 'INCURRED_LOSS', 'GROUND_UP_PAID']
    selcols = mcols + grpcols + fincols
    ordercols = mcols + grpcols
    
    return (
        filtered
        .groupby(mcols + grpcols)
        .agg(
            F.sum('FIRM_PAID').alias('FIRM_PAID'),
            F.sum('ALAS_PAID').alias('PAID_LOSS'),
            F.sum('TOTAL_INCURRED').alias('INCURRED_LOSS'),
            F.sum('GROUND_UP_PAID').alias('GROUND_UP_PAID'),
        )
        .select(selcols)
        .orderBy(ordercols)
    )


def calculate_mom_financials(agg_df, grpcols):
    """
    Calculates month-over-month changes for financial metrics.
    
    Args:
        agg_df: Aggregated financial DataFrame
        grpcols: List of grouping columns
    
    Returns:
        DataFrame with month-over-month changes
    """
    mcols = ['QUARTER_END']
    aggcols = ['FIRM_PAID', 'PAID_LOSS', 'INCURRED_LOSS', 'GROUND_UP_PAID']
    
    # Calculate MOM changes
    momdf = mom_change(agg_df.drop('CL01_COUNT') if 'CL01_COUNT' in agg_df.columns else agg_df, 
                       mcols, grpcols, aggcols)
    
    # Join back with original to preserve other columns
    return (
        momdf
        .join(agg_df.select(*mcols, *grpcols), on=mcols + grpcols, how='left')
        .orderBy(mcols + grpcols)
    )


def calculate_cl01_count(snapshots_df, lob_filter, grpcols):
    """
    Calculates CL01 (open claims) count for a specific LOB.
    
    Args:
        snapshots_df: Input DataFrame with snapshot data
        lob_filter: SQL WHERE clause to filter LOB
        grpcols: List of grouping columns
    
    Returns:
        DataFrame with CL01 counts
    """
    df = snapshots_df.where(lob_filter)
    mcols = ['MONTH_END']
    
    return (
        df
        .groupby(mcols + grpcols)
        .agg(
            F.sum(
                F.when(
                    (F.col('MATTER_CATEGORY_SK') == 2) & (F.col('MATTER_STATUS_CODE') == 'Open'), 
                    1
                ).otherwise(0)
            ).alias('CL01_COUNT')
        )
    )

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
    """Aggregate LPL financials at quarterly level"""
    snapshots = dlt.read('msnapshots_bronze_cl01')
    grpcols = ['PRIMARY_CAUSE_OF_LOSS_SK', 'AOP_SK']
    
    return (
        aggregate_financials_by_lob(snapshots, 'LOB_SK = 1', grpcols, MONTH_END)
        .withColumn('LOB_SK', F.lit(1))
        .withColumn('CLAIMANT_TYPE_SK', F.lit(-2))
    )


@dlt.table
@dlt.expect_or_fail('no null dates', 'QUARTER_END IS NOT NULL')
def silver_financials_agg_supp():
    """Aggregate supplemental financials at quarterly level"""
    snapshots = dlt.read('msnapshots_bronze_cl01')
    grpcols = ['PRIMARY_CAUSE_OF_LOSS_SK', 'CLAIMANT_TYPE_SK']
    
    return (
        aggregate_financials_by_lob(snapshots, 'LOB_SK > 1', grpcols, MONTH_END)
        .withColumn('LOB_SK', F.lit(4))
        .withColumn('AOP_SK', F.lit(-2))
    )

# COMMAND ----------

# MAGIC %md ### Silver Financials MOM

# COMMAND ----------

@dlt.table
@dlt.expect_or_fail('no null dates', 'QUARTER_END IS NOT NULL')
def silver_financials_mom_lpl():
    """Calculates the monthly LPL loss emergence as period n+1 totals - period n totals"""
    tbl = dlt.read('silver_financials_agg_lpl')
    grpcols = ['PRIMARY_CAUSE_OF_LOSS_SK', 'AOP_SK']
    
    return (
        calculate_mom_financials(tbl, grpcols)
        .withColumn('LOB_SK', F.lit(1))
        .withColumn('CLAIMANT_TYPE_SK', F.lit(-2))
    )


@dlt.table
@dlt.expect_or_fail('no null dates', 'QUARTER_END IS NOT NULL')
def silver_financials_mom_supp():
    """Calculates the monthly supplemental loss emergence as period n+1 totals - period n totals"""
    tbl = dlt.read('silver_financials_agg_supp')
    grpcols = ['PRIMARY_CAUSE_OF_LOSS_SK', 'CLAIMANT_TYPE_SK']
    
    return (
        calculate_mom_financials(tbl, grpcols)
        .withColumn('LOB_SK', F.lit(4))
        .withColumn('AOP_SK', F.lit(-2))
    )

# COMMAND ----------

# TESTING CODE - delete when complete 8/11/2025

#import pyspark.sql.functions as F
#CATALOG = 'test_catalog'
#SCHEMA = 'claims_trends'

#sdf = spark.table(f'{CATALOG}.{SCHEMA}.msnapshots_bronze_cl01')
#display(sdf.select('QUARTER_END').distinct().orderBy(F.desc('QUARTER_END')))

# COMMAND ----------

# MAGIC %md ### Silver CL01 Count

# COMMAND ----------

@dlt.table
def silver_cl01_lpl():
    """Creates silver CL01 count table for LPL"""
    snapshots = dlt.read('msnapshots_bronze_cl01')
    grpcols = ['LOB_SK', 'PRIMARY_CAUSE_OF_LOSS_SK', 'AOP_SK']
    
    return (
        calculate_cl01_count(snapshots, 'LOB_SK = 1', grpcols)
        .withColumn('LOB_SK', F.lit(1))
        .withColumn('CLAIMANT_TYPE_SK', F.lit(-2))
    )

# COMMAND ----------

@dlt.table
def silver_cl01_supp():
    """Creates silver CL01 count table for supplemental"""
    snapshots = dlt.read('msnapshots_bronze_cl01')
    grpcols = ['PRIMARY_CAUSE_OF_LOSS_SK', 'CLAIMANT_TYPE_SK']
    
    return (
        calculate_cl01_count(snapshots, 'LOB_SK > 1', grpcols)
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

# MAGIC %md ## Ad Hoc Testing

# COMMAND ----------

if False:
    # test silver layer
    import pyspark.sql.functions as F

    lpl = spark.table('test_catalog.claims_trends.silver_financials_agg_lpl')
    display(lpl.groupBy('QUARTER_END').agg(F.count('AOP_SK').alias('ROWCOUNT')).orderBy(F.desc('QUARTER_END')))

    # test bronze layer - this has matters from 2025-06-30

    lpl = spark.table('test_catalog.claims_trends.msnapshots_bronze_cl01')
    #display(lpl.groupBy('QUARTER_END').agg(F.count('AOP_SK').alias('MATTER_COUNT')).orderBy(F.desc('QUARTER_END')))

    display(lpl.select('QUARTER_END').distinct())

# COMMAND ----------
