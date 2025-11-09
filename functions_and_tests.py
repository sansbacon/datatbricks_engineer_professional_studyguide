# Databricks notebook source
# MAGIC %md ## Helper Functions and Tests
# MAGIC
# MAGIC This notebook contains all helper functions and their associated tests for the Claims Trends DLT pipeline.

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

# MAGIC %md ### Add Quarter End Function

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

# COMMAND ----------

# MAGIC %md ### Aggregate Financials Function

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

# COMMAND ----------

# MAGIC %md ### Add Claim Indicators Function

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

# COMMAND ----------

# MAGIC %md ### Get Snapshot Periods Function

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

# COMMAND ----------

# MAGIC %md ### Month-over-Month Change Function

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

# COMMAND ----------

# MAGIC %md ### Quarter End Check Function

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

# COMMAND ----------

# MAGIC %md ### Helper Functions for DLT Pipeline

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

# MAGIC %md ## Run All Tests

# COMMAND ----------

# Run tests if needed
RUN_TESTS = False

try:
    RUN_TESTS = spark.conf.get('claims_trends.run_tests') == 'Yes'
except:
    try:
        RUN_TESTS = dbutils.widgets.get('RUN_TESTS') == 'Yes'
    except:
        pass

if RUN_TESTS:
    print("Running all tests...")
    test_add_quarter_end()
    print("✓ test_add_quarter_end passed")
    
    test_aggregate_financials()
    print("✓ test_aggregate_financials passed")
    
    test_add_claim_indicators()
    print("✓ test_add_claim_indicators passed")
    
    test_get_snapshot_periods()
    print("✓ test_get_snapshot_periods passed")
    
    test_mom_change()
    print("✓ test_mom_change passed")
    
    test_is_previous_month_end_quarter_end()
    print("✓ test_is_previous_month_end_quarter_end passed")
    
    print("\nAll tests passed successfully!")

# COMMAND ----------
