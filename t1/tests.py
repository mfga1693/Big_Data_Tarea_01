# =========================================================
# Imports
# =========================================================

import pytest

from pyspark.sql import Row
from datetime import date

from pyspark.sql.types import (
    StructField,
    StructType,
    DateType,
    FloatType,
    IntegerType,
    StringType
)

from functions.functions import (
    load_cyclists,
    load_routes,
    load_activities,
    join_data,
    calculate_total_kilometers,
    calculate_daily_average,
    calculate_province_totals,
    get_top_cyclists_by_total_km,
    get_top_cyclists_by_daily_average,
    validate_dataframe
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
    joined_data = [
        (117860659, "Liz", "San Jose", date(2026, 5, 1), 18.5),
        (117860659, "Liz", "San Jose", date(2026, 5, 2), 32.0),
        (402200492, "Fernanda", "Alajuela", date(2026, 5, 1), 24.5),
        (402500696, "Andres", "Cartago", date(2026, 5, 3), 40.0),
    ]

    df = spark_session.createDataFrame(
        joined_data,
        [
            "cedula",
            "nombre_completo",
            "provincia",
            "fecha",
            "kilometros"
        ]
    )

    result = calculate_province_totals(df)

    
    totals = {
        row.provincia: row.total_kilometros_provincia
        for row in result.collect()
    }

    assert totals["San Jose"] == 50.5
    assert totals["Alajuela"] == 24.5
    assert totals["Cartago"] == 40.0


# =========================================================
# Ranking tests
# =========================================================
#Fer
def test_get_top_cyclists_by_total_km(spark_session):
    """
    Prueba el top de ciclistas según el total de kilómetros recorridos por provincia.
    """

    total_km_data = [
        (402200492, "Liz", "San Jose", 100.0),
        (402200493, "Fernanda", "San Jose", 200.0),
        (402200494, "Andres", "San Jose", 300.0),
        (402200495, "Jesus", "San Jose", 400.0),
        (402200496, "Rodrigo", "San Jose", 500.0),
        (402200497, "Paula", "San Jose", 600.0),
        (402200498, "Theo", "San Jose", 700.0),

        (402200499, "Juan", "Alajuela", 100.0),
        (402200411, "Erick", "Alajuela", 200.0),
        (402200412, "Mauricio", "Alajuela", 300.0),
        (402200413, "Federico", "Alajuela", 400.0),
        (402200414, "Mariela", "Alajuela", 500.0),
        (402200415, "Ariana", "Alajuela", 600.0),
        (402200416, "Mariana", "Alajuela", 700.0),
        (402200417, "Celeste", "Alajuela", 800.0),
    ]  

    df = spark_session.createDataFrame(
        total_km_data,
        [
            "cedula",
            "nombre_completo",
            "provincia",
            "total_kilometros"
        ]
    )

    result = get_top_cyclists_by_total_km(df, top_n = 5)

    rows = result.collect()

    san_jose = [
        row for row in rows
        if row.provincia == "San Jose"
    ] 

    alajuela = [
        row for row in rows
        if row.provincia == "Alajuela"
    ] 

    #Se valida la cantidad de datos del top N
    assert len(san_jose) == 5
    assert len(alajuela) == 5

    #Se ordena los datos
    san_jose = sorted(
        san_jose,
        key=lambda x: x.total_kilometros,
        reverse=True
    )

    alajuela = sorted(
        alajuela,
        key=lambda x: x.total_kilometros,
        reverse=True
    )

    # Validar ranking San Jose
    assert san_jose[0].nombre_completo == "Theo"
    assert san_jose[1].nombre_completo == "Paula"
    assert san_jose[2].nombre_completo == "Rodrigo"
    assert san_jose[3].nombre_completo == "Jesus"
    assert san_jose[4].nombre_completo == "Andres"

    # Validar ranking Alajuela
    assert alajuela[0].nombre_completo == "Celeste"
    assert alajuela[1].nombre_completo == "Mariana"
    assert alajuela[2].nombre_completo == "Ariana"
    assert alajuela[3].nombre_completo == "Mariela"
    assert alajuela[4].nombre_completo == "Federico"



#Fer
def test_get_top_cyclists_by_daily_average(spark_session):
    """
    Prueba el top de ciclistas según el promedio diario de kilómetros recorridos por provincia.
    """

    daily_average_data = [
        (117860659, "Liz", "San Jose", 25.5),
        (402200492, "Fernanda", "San Jose", 42.0),
        (402500696, "Andres", "San Jose", 30.0),

        (301110111, "Carlos", "Alajuela", 50.0),
        (401220333, "Maria", "Alajuela", 22.5),
        (501330444, "Sofia", "Alajuela", 45.0),
    ]

    df = spark_session.createDataFrame(
        daily_average_data,
        [
            "cedula", 
            "nombre_completo", 
            "provincia", 
            "promedio_diario_kilometros"
        ]
    )

    result = get_top_cyclists_by_daily_average(df, top_n=2)
    rows = result.collect()

    san_jose = [
        row for row in rows
        if row.provincia == "San Jose"
    ]

    alajuela = [
        row for row in rows
        if row.provincia == "Alajuela"
    ]

    #Se valida la cantidad de datos del top N
    assert len(san_jose) == 2
    assert len(alajuela) == 2

    #Se ordena los datos
    san_jose = sorted(
        san_jose,
        key=lambda x: x.promedio_diario_kilometros,
        reverse=True
    )

    alajuela = sorted(
        alajuela,
        key=lambda x: x.promedio_diario_kilometros,
        reverse=True
    )

    # Validar ranking San Jose
    assert san_jose[0].nombre_completo == "Fernanda"
    assert san_jose[1].nombre_completo == "Andres"

    # Validar ranking Alajuela
    assert alajuela[0].nombre_completo == "Carlos"
    assert alajuela[1].nombre_completo == "Sofia"


# =========================================================
# Edge case tests
# =========================================================
#Fer
def test_empty_dataframe(spark_session):
    """
    Prueba el comportamiento de un dataframe vacío.
    """
    empty_df = spark_session.createDataFrame(
        [], StructType(
                [
                    StructField("cedula", IntegerType()),
                    StructField("nombre_completo", StringType()),
                    StructField("provincia", StringType()),
                    StructField("fecha", DateType()),
                    StructField("kilometros", FloatType())
                ]
            )
    )
    
    result = validate_dataframe(empty_df)

    assert result == False

#Fer
def test_null_values(spark_session):
    """
    Prueba el manejo de valores nulos.
    """

    data_with_nulls = [
        (117860659, None, "San Jose", date(2026, 5, 1), 18.5),
        (402200492, "Fernanda", "Alajuela", date(2026, 5, 1), None),
        (402500696, "Andres", None, date(2026, 5, 3), 40.0),
    ]

    df = spark_session.createDataFrame(
        data_with_nulls,
        [
            "cedula",
            "nombre_completo",
            "provincia",
            "fecha",
            "kilometros"
        ]
    )

    result = validate_dataframe(df)

    assert result == True

#Fer
def test_duplicate_activities(spark_session):
    """
    Prueba el manejo de actividades duplicadas.
    """

    data_with_duplicates = [
        (117860659, "Liz", "San Jose", date(2026, 5, 1), 18.5),
        (117860659, "Liz", "San Jose", date(2026, 5, 1), 18.5),
        (402200492, "Fernanda", "Alajuela", date(2026, 5, 1), 24.5),
        (402500696, "Andres", "Cartago", date(2026, 5, 3), 40.0),
    ]

    df = spark_session.createDataFrame(
        data_with_duplicates,
        [
            "cedula",
            "nombre_completo",
            "provincia",
            "fecha",
            "kilometros"
        ]
    )

    result = validate_dataframe(df)

    assert result == True