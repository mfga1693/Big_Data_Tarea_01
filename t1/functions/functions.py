#Imports necesarios

#Pyspark para manipulacion de DataFrames
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

#Para funciones de agregacion y ordenamiento
from pyspark.sql.functions import col, sum, avg, count, desc, dense_rank

#Para definir esquemas de los DataFrames
#Evitar que Spark infiera los tipos de datos incorrectamente
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType

#Para definir ventanas de ranking
from pyspark.sql.window import Window

#Definir esquemas para los DataFrames de ciclista, ruta y actividad, para eviatr que spark los infiera
#Definir esquemas para el dataframe de clicista.csv
#cedula, nombre y provincia del ciclista
schema_ciclista = StructType([ #StructType define la estructura del DataFrame
    StructField("cedula", IntegerType(), False), #StructField define cada columna del DtaFrame
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
"""
Estas funciones reciben la SparkSession y la ruta del archivo .csv, 
lee el csv sin encabezado (header=False) y con el esquema definido (schema=schema_xxx),
y retorna un DataFrame con la estructura definida en el schema.
"""
#Funcion de carga para cilista.csv
def load_cyclists(spark, file_path):
    """Carga ciclista.csv y retorna un DataFrame."""
    return spark.read.csv(file_path, header=False, schema=schema_ciclista)

#Funcion de carga para ruta.csv
def load_routes(spark, file_path):
    """Carga ruta.csv y retorna un DataFrame."""
    return spark.read.csv(file_path, header=False, schema=schema_ruta)

#Funcion de carga para actividad.csv
def load_activities(spark, file_path):
    """Carga actividad.csv y retorna un DataFrame."""
    return spark.read.csv(file_path, header=False, schema=schema_actividad)


#Funciones para hacer el joinS entre los DataFrames
"""
Importante: se usa left join para mantener todos los ciclistas aunque no tengan actividades o rutas asociadas
"""

#Funcion para hacer el join entre dos DataFrames
def join_dataframes(df1, df2, on_column, how="left"):
    """
    Esta función recibe dos DataFrames, la columna por la que se va a hacer el join y el tipo de join (por defecto "left"),
    y retorna el DataFrame resultante del join.
    """
    return df1.join(df2, on=on_column, how=how)

#Funcion para hacer el join entre los tres DataFrames 
def join_data(ciclistas_df, rutas_df, actividades_df):
    """
    Esta funvion une los 3 dataframes en uno, primero se une ciclistas con actividades por cedula, luego se une el resultado con rutas, 
    por codigo_rutalo que da un DataFrame completo con toda la información de ciclistas, actividades y rutas. 
    """
    #Primero se unen ciclistas con actividades
    #Unir ciclistas con actividades
    ciclistas_actividades_df = join_dataframes(ciclistas_df, actividades_df, on_column="cedula", how="left")
    
    #Luego se une el resultado con rutas
    #Unir el resultado con rutas
    resultado_df = join_dataframes(ciclistas_actividades_df, rutas_df, on_column="codigo_ruta", how="left")
    
    return resultado_df


#Funciones para los cálculos

#Funcion para calcular el total de kilometros recorridos por cada ciclista
def calculate_total_kilometers(joined_df):
    """
    Esta funcion grupa por cedula y nombre completo, luego suma los kilometros de las rutas asociadas a cada ciclista
    El "groupBy" agrupa los datos por ciclista, y el "agg" aplica la función de suma a la columna de kilometros, lo que termina en "total_kilometros"
    """
    return joined_df.groupBy("cedula", "nombre_completo", "provincia").agg(sum("kilometros").alias("total_kilometros"))

#Funcion para calcular el total de kilometros recorridos por provincia
def calculate_province_totals(joined_df):
    """
    Esta funcion agrupa por provincia, luego suma los kilometros de las rutas asociadas a cada provincia
    El "groupBy" agrupa los datos por provincia, y el "agg" aplica la función de suma a la columna de kilometros, lo que termina en "total_kilometros_provincia"
    """
    return joined_df.groupBy("provincia").agg(sum("kilometros").alias("total_kilometros_provincia"))

#Funcion para calcular el promedio diario de kilometros recorridos por cada ciclista
def calculate_daily_average(joined_df):
    """
    Esta funcion agrupa por cedula y nombre completo, luego calcula el promedio de kilometros por dia
    El "groupBy" agrupa los datos por ciclista, y el "agg" aplica la función de promedio a la columna de kilometros, lo que termina en "promedio_diario_kilometros"
    """
    Km_por_dia = joined_df.groupBy("cedula", "nombre_completo", "provincia", "fecha").agg(sum("kilometros").alias("kilometros_por_dia"))
    return Km_por_dia.groupBy("cedula", "nombre_completo", "provincia").agg(avg("kilometros_por_dia").alias("promedio_diario_kilometros"))



#Funcion para obtener el tops 
""" 
Importante:
.orderBy(desc("columna")):Ordena el DataFrame por total de kilometros en orden descendente 
.limit(n):limita el resultado
.partitionBy :es como hacer un groupBy pero para el ranking
"""

#Funcion para obtener el top ciclistas segun el total de kilometros recorridos por provincia
def get_top_cyclists_by_total_km(total_km_df, n=5):
    """
    Recibe el DataFrame con totales por ciclista y retorna el top N por provincia.
    Usa una Window function para rankear dentro de cada provincia por km totales
    en orden descendente. Sin esto, el ranking sería global y no por provincia.
    """
    window_spec = Window.partitionBy("provincia").orderBy(desc("total_kilometros"))
    ranked_df = total_km_df.withColumn("rank", dense_rank().over(window_spec))
    return ranked_df.filter(col("rank") <= n).drop("rank")

#Funcion para obtener el top ciclistas segun el promedio diario de kilometros recorridos por provincia
def get_top_cyclists_by_daily_average(daily_average_df, n=5):
    """
    Esta función recibe el DataFrame con promedios diarios por ciclista y retorna el top N por provincia.
    Usa una Window function para rankear dentro de cada provincia por promedio diario de km en orden
    descendente. Sin esto, el ranking sería global y no por provincia.
    """
    window_spec = Window.partitionBy("provincia").orderBy(desc("promedio_diario_kilometros"))
    ranked_df = daily_average_df.withColumn("rank", dense_rank().over(window_spec))
    return ranked_df.filter(col("rank") <= n).drop("rank")  



