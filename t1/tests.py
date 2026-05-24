# =========================================================
# Imports
# =========================================================

import pytest

from pyspark.sql import Row
from datetime import date

from pyspark.sql.types import (
    DateType,
    FloatType,
    IntegerType,
    StringType
)

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

#Función auxiliar
def assert_column_type(df, column_name, expected_type):
    """
    Valida que una columna exista en el df y que su tipo de dato coincida con el tipo esperado.
    """

    field = next(
        item for item in df.schema.fields
        if item.name == column_name
    )

    assert isinstance(field.dataType, expected_type)


# =========================================================
# 1. Load function tests
# =========================================================

#1.1
def test_load_cyclists(spark_session, tmp_path):
    """
    Prueba la función que carga el archivo de ciclistas: valida la lectura del cvs, el esquema y la cantidad de registros de ciclistas.
    """

    file_path = tmp_path / "ciclista.csv"
    file_path.write_text(
        "117860659,Liz,San Jose\n"
        "402200492,Fernanda,Heredia\n"
        "402500696,Andres,Heredia\n",
        encoding="utf-8"
    )

    cyclists_df = load_cyclists(spark_session, str(file_path))
    assert cyclists_df.count() == 3
    assert cyclists_df.columns == ["cedula", "nombre_completo", "provincia"]
    assert_column_type(cyclists_df, "cedula", IntegerType)
    assert_column_type(cyclists_df, "nombre_completo", StringType)
    assert_column_type(cyclists_df, "provincia", StringType)


#1.2
def test_load_routes(spark_session, tmp_path):
    """
    Prueba la función que carga el archivo de rutas:  valida la lectura del cvs, el esquema y la la cantidad de registros de rutas.
    """

    file_path = tmp_path / "ruta.csv"
    file_path.write_text(
        "101,Ruta Escazu,18.5\n"
        "102,Ruta Cartago,32.0\n"
        "103,Ruta Grecia,24.5\n",
        encoding="utf-8"
    )

    routes_df = load_routes(spark_session, str(file_path))
    assert routes_df.count() == 3
    assert routes_df.columns == ["codigo_ruta", "nombre_ruta", "kilometros"]
    assert_column_type(routes_df, "codigo_ruta", IntegerType)
    assert_column_type(routes_df, "nombre_ruta", StringType)
    assert_column_type(routes_df, "kilometros", FloatType)

#1.3
def test_load_activities(spark_session, tmp_path):
    """
    Prueba la función que carga el archivo de actividades: valida la lectura del csv, el esquema y la cantidad de registros de actividades.
    """

    file_path = tmp_path / "actividad.csv"
    file_path.write_text(
        "101,117860659,2026-05-01\n"
        "102,402200492,2026-05-02\n"
        "103,402500696,2026-05-03\n",
        encoding="utf-8"
    )

    activities_df = load_activities(spark_session, str(file_path))
    assert activities_df.count() == 3
    assert activities_df.columns == ["codigo_ruta", "cedula", "fecha"]
    assert_column_type(activities_df, "codigo_ruta", IntegerType)
    assert_column_type(activities_df, "cedula", IntegerType)
    assert_column_type(activities_df, "fecha", DateType)


# =========================================================
# 2. Join tests
# =========================================================

#2.1
def test_join_data(spark_session):
    """
    Prueba que los dfs de ciclistas, rutas y actividades se unan correctamente: verificando el esquema,
    los tipos de datos y la cantidad de registros.
    """

    cyclists_data = [
        (117860659, "Liz", "San Jose"),
        (402200492, "Fernanda", "Alajuela"),
        (402500696, "Andres", "Cartago"),
    ]

    routes_data = [
        (101, "Ruta Escazu", 18.5),
        (102, "Ruta Cartago", 32.0),
        (103, "Ruta Grecia", 24.5),
    ]

    activities_data = [
        (101, 117860659, date(2026, 5, 1)),
        (102, 402200492, date(2026, 5, 2)),
        (103, 402500696, date(2026, 5, 3)),
    ]

    cyclists_df = spark_session.createDataFrame(
        cyclists_data,
        ["cedula", "nombre_completo", "provincia"]
    )

    routes_df = spark_session.createDataFrame(
        routes_data,
        ["codigo_ruta", "nombre_ruta", "kilometros"]
    )

    activities_df = spark_session.createDataFrame(
        activities_data,
        ["codigo_ruta", "cedula", "fecha"]
    )

    result_df = join_data(cyclists_df, routes_df, activities_df)

    assert result_df.count() == 3

    liz_row = result_df.filter(result_df.cedula == 117860659).first()

    assert liz_row.nombre_completo == "Liz"
    assert liz_row.provincia == "San Jose"
    assert liz_row.codigo_ruta == 101
    assert liz_row.nombre_ruta == "Ruta Escazu"
    assert liz_row.kilometros == pytest.approx(18.5)


# =========================================================
# 3. Aggregation tests
# =========================================================

#3.1
def test_calculate_total_kilometers(spark_session):
    """
    Prueba el cálculo del total de kilómetros recorridos por ciclista.
    """

    joined_data = [
        (117860659, "Liz", "San Jose", date(2026, 5, 1), 18.5),
        (117860659, "Liz", "San Jose", date(2026, 5, 2), 32.0),
        (402200492, "Fernanda", "Alajuela", date(2026, 5, 1), 24.5),
        (402500696, "Andres", "Cartago", date(2026, 5, 3), 40.0),
    ]

    joined_df = spark_session.createDataFrame(
        joined_data,
        ["cedula", "nombre_completo", "provincia", "fecha", "kilometros"]
    )

    result_df = calculate_total_kilometers(joined_df)

    totals = {
        row.cedula: row.total_kilometros
        for row in result_df.collect()
    }

    assert result_df.count() == 3
    assert totals[117860659] == pytest.approx(50.5)
    assert totals[402200492] == pytest.approx(24.5)
    assert totals[402500696] == pytest.approx(40.0)

#3.2
def test_calculate_daily_average(spark_session):
    """
    Prueba el cálculo del promedio diario de kilómetros por ciclista.
    """

    joined_data = [
        # Liz realiza dos rutas el mismo día.
        # Primero deben sumarse: 18.5 + 24.5 = 43.0 km.
        (117860659, "Liz", "San Jose", date(2026, 5, 1), 18.5),
        (117860659, "Liz", "San Jose", date(2026, 5, 1), 24.5),

        # Segundo día de Liz: 32.0 km.
        (117860659, "Liz", "San Jose", date(2026, 5, 2), 32.0),

        # Fernanda tiene una actividad en un único día.
        (402200492, "Fernanda", "Alajuela", date(2026, 5, 1), 30.0),

        # Andres tiene una actividad en un único día.
        (402500696, "Andres", "Cartago", date(2026, 5, 3), 40.0),
    ]

    joined_df = spark_session.createDataFrame(
        joined_data,
        ["cedula", "nombre_completo", "provincia", "fecha", "kilometros"]
    )

    result_df = calculate_daily_average(joined_df)

    averages = {
        row.cedula: row.promedio_diario_kilometros
        for row in result_df.collect()
    }

    assert result_df.count() == 3

    # Liz:
    # Día 1 = 18.5 + 24.5 = 43.0 km
    # Día 2 = 32.0 km
    # Promedio = (43.0 + 32.0) / 2 = 37.5
    assert averages[117860659] == pytest.approx(37.5)

    # Fernanda:
    # Solo tiene un día con 30.0 km.
    assert averages[402200492] == pytest.approx(30.0)

    # Andres:
    # Solo tiene un día con 40.0 km.
    assert averages[402500696] == pytest.approx(40.0)


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