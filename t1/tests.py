# =========================================================
# Imports
# =========================================================

import pytest

from pyspark.sql import Row

from functions import (
    load_cyclists,
    load_routes,
    load_activities,
    join_data,
    calculate_total_kilometers,
    calculate_daily_average,
    calculate_province_totals,
    get_top_cyclists_by_total_km,
    get_top_cyclists_by_daily_average
)

# =========================================================
# Load function tests
# =========================================================

#Liz
def test_load_cyclists(spark_session):
    """
    Test cyclist loading function.
    """

    # TODO:
    #
    # Load CSV
    # Validate dataframe is not empty
    # Validate schema
    # Validate row count

    pass


#Liz
def test_load_routes(spark_session):
    """
    Test routes loading function.
    """

    # TODO

    pass


#Liz
def test_load_activities(spark_session):
    """
    Test activities loading function.
    """

    # TODO

    pass


# =========================================================
# Join tests
# =========================================================

#Liz
def test_join_data(spark_session):
    """
    Test dataframe joins.
    """

    # -----------------------------------------------------
    # Create test data
    # -----------------------------------------------------

    cyclists_data = [
        (1, "John Doe", "San Jose")
    ]

    routes_data = [
        (100, "Route A", 25.5)
    ]

    activities_data = [
        (100, 1, "2026-05-01")
    ]

    # -----------------------------------------------------
    # Create dataframes
    # -----------------------------------------------------

    # TODO:
    #
    # cyclists_df = ...
    # routes_df = ...
    # activities_df = ...

    # -----------------------------------------------------
    # Execute join
    # -----------------------------------------------------

    # TODO:
    #
    # result_df = ...

    # -----------------------------------------------------
    # Assertions
    # -----------------------------------------------------

    # TODO:
    #
    # assert result_df.count() == 1

    pass


# =========================================================
# Aggregation tests
# =========================================================
#Liz
def test_calculate_total_kilometers(spark_session):
    """
    Test total kilometers aggregation.
    """

    # TODO:
    #
    # Create dataframe
    # Execute aggregation
    # Validate totals

    pass

#Liz
def test_calculate_daily_average(spark_session):
    """
    Test daily average calculation.
    """

    # TODO

    pass

#Fer
def test_calculate_province_totals(spark_session):
    """
    Test province totals aggregation.
    """

    # TODO

    pass


# =========================================================
# Ranking tests
# =========================================================
#Fer
def test_get_top_cyclists_by_total_km(spark_session):
    """
    Test top cyclists ranking by total kilometers.
    """

    # TODO:
    #
    # Create dataframe
    # Execute ranking
    # Validate order
    # Validate top N

    pass

#Fer
def test_get_top_cyclists_by_daily_average(spark_session):
    """
    Test top cyclists ranking by daily average.
    """

    # TODO

    pass


# =========================================================
# Edge case tests
# =========================================================
#Fer
def test_empty_dataframe(spark_session):
    """
    Test empty dataframe behavior.
    """

    # TODO

    pass

#Fer
def test_null_values(spark_session):
    """
    Test null handling.
    """

    # TODO

    pass

#Fer
def test_duplicate_activities(spark_session):
    """
    Test duplicated activity handling.
    """

    # TODO

    pass