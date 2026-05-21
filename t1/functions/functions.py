# Imports necesarios
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum,
    avg,
    count,
    desc
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType
from pyspark.sql.functions import dense_rank
from pyspark.sql.window import Window

#Definir esquemas para el dataframe de clicista.csv
#cedula, nombre y provincia del ciclista
#StructType define la estructura del DataFrame
#StructField define cada columna del DtaFrame
schema_ciclista = StructType([
    StructField("cedula", IntegerType(), False),
    StructField("nombre_completo",StringType(), False),
    StructField("provincia", StringType(), False)
])

#Definir esquemas para el dataframe de ruta.csv
#codigo de la ruta, nombre y kilometros
schema_ruta = StructType([
    StructField("codigo_ruta", IntegerType(), False),
    StructField("nombre_ruta", StringType(), False),
    StructField("kilometros", FloatType(), False)
])

#Definir esquemas para el dataframe de actividad.csv
#codigo ruta, cedula y fecha
schema_actividad = StructType([
    StructField("codigo_ruta", IntegerType(), False),
    StructField("cedula", IntegerType(), False),
    StructField("fecha", DateType(), False)
])

#Funciones de carga

#Funcion de carga para cilista.csv
#La funcion recibe la spark session y la ruta de archivo, luego retorna el DataFrame
def load_cyclists(spark, file_path):
    return spark.read.csv(file_path, header=False, schema=schema_ciclista)

#Funcion de carga para ruta.csv
def load_routes(spark, file_path):
    return spark.read.csv(file_path, header=False, schema=schema_ruta)

#Funcion de carga para actividad.csv
def load_activities(spark, file_path):
    return spark.read.csv(file_path, header=False, schema=schema_actividad)


#Funcion para hacer el join entre los DataFrames
#Uso left join para mantener todos los ciclistas aunque no tengan actividades o rutas asociadas

#Funcion para hacer el join entre dos DataFrames
def join_dataframes(df1, df2, on_column, how="left"):
    return df1.join(df2, on=on_column, how=how)

#Funcion para hacer el join entre los tres DataFrames 
def join_data(ciclistas_df, rutas_df, actividades_df):
    #Primero se unen ciclistas con actividades
    #Unir ciclistas con actividades
    ciclistas_actividades_df = join_dataframes(ciclistas_df, actividades_df, on_column="cedula", how="left")
    
    #Luego se une el resultado con rutas
    #Unir el resultado con rutas
    resultado_df = join_dataframes(ciclistas_actividades_df, rutas_df, on_column="codigo_ruta", how="left")
    
    return resultado_df

#Funciones para los cálculos

#Funcion para calcular el total de kilometros recorridos por cada ciclista
#Agrupa por cedula y nombre completo, luego suma los kilometros de las rutas asociadas a cada ciclista
#El "groupBy" agrupa los datos por ciclista, y el "agg" aplica la función de suma a la columna de kilometros, lo que termina en "total_kilometros"
def calculate_total_kilometers(joined_df):
    return joined_df.groupBy("cedula", "nombre_completo", "provincia").agg(sum("kilometros").alias("total_kilometros"))


#Funcion para calcular el total de kilometros recorridos por provincia
#Agrupa por provincia, luego suma los kilometros de las rutas asociadas a cada provincia
#El "groupBy" agrupa los datos por provincia, y el "agg" aplica la función de suma a la columna de kilometros, lo que termina en "total_kilometros_provincia"
def calculate_province_totals(joined_df):
    return joined_df.groupBy("provincia").agg(sum("kilometros").alias("total_kilometros_provincia"))

#Funcion para calcular el promedio diario de kilometros recorridos por cada ciclista
#Agrupa por cedula y nombre completo, luego calcula el promedio de kilometros por dia
#El "groupBy" agrupa los datos por ciclista, y el "agg" aplica la función de promedio a la columna de kilometros, lo que termina en "promedio_diario_kilometros"
def calculate_daily_average(joined_df):
    Km_por_dia = joined_df.groupBy("cedula", "nombre_completo", "provincia", "fecha").agg(sum("kilometros").alias("kilometros_por_dia"))
    return Km_por_dia.groupBy("cedula", "nombre_completo", "provincia").agg(avg("kilometros_por_dia").alias("promedio_diario_kilometros"))

#Funcion para obtener el top ciclistas segun el total de kilometros recorridos por provincia
#.orderBy(desc("columna")):Ordena el DataFrame por total de kilometros en orden descendente 
#.limit(n):limita el resultado
#.partitionBy :es como hacer un groupBy pero para el ranking
#Primero hay que definir la ventana de ranking, que se hace con Window.partitionBy("provincia").orderBy(desc("total_kilometros_provincia"))
def get_top_cyclists_by_total_km(total_km_df, n=5):
    window_spec = Window.partitionBy("provincia").orderBy(desc("total_kilometros"))
    ranked_df = total_km_df.withColumn("rank", dense_rank().over(window_spec))
    return ranked_df.filter(col("rank") <= n).drop("rank")

def get_top_cyclists_by_daily_average(daily_average_df, n=5):
    window_spec = Window.partitionBy("provincia").orderBy(desc("promedio_diario_kilometros"))
#Luego se aplica la función de ranking a cada fila del DataFrame, lo que asigna
#un número de ranking a cada ciclista dentro de su provincia, basado en el total de kilometros recorridos
    ranked_df = daily_average_df.withColumn("rank", dense_rank().over(window_spec))
    return ranked_df.filter(col("rank") <= n).drop("rank")  



