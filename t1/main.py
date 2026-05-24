# =========================================================
# Imports
# =========================================================

import sys
import os
from pyspark.sql import SparkSession

# =========================================================
# Custom imports
# =========================================================

from functions.functions import (
    load_cyclists, 
    load_routes, 
    load_activities,
    join_data, 
    calculate_total_kilometers,
    calculate_daily_average, 
    get_top_cyclists_by_total_km,
    get_top_cyclists_by_daily_average
)
# =========================================================
# Constants
# =========================================================

NUM_ARGUMENTS = 3
TOP_N = 5

# =========================================================
# Helper functions
# =========================================================

def validate_arguments():
    """
    Valida que se hayan pasado los argumentos necesarios para ejecutar el programa
    """
    # Verificar el número de argumentos
    if len(sys.argv) != NUM_ARGUMENTS + 1:
        raise ValueError(f"Error: Se requieren {NUM_ARGUMENTS} argumentos. argumentos: ciclista.csv ruta.csv actividad.csv")
    
    # Verificar que los archivos existan y tienen la extensión .csv
    for i in sys.argv[1:]:
        if not os.path.isfile(i):
            raise FileNotFoundError(f"Error: El archivo {i} no existe.")
        if not i.endswith(".csv"):
            raise ValueError(f"Error: El archivo {i} no es un archivo CSV.")
    
# =========================================================
# Main function
# =========================================================
def main():
    # -----------------------------------------------------
    # Validate arguments
    # -----------------------------------------------------
    validate_arguments()
    # -----------------------------------------------------
    # Read command line arguments
    # -----------------------------------------------------
    archivo_ciclistas = sys.argv[1]
    archivo_rutas = sys.argv[2]
    archivo_actividades = sys.argv[3]
    # -----------------------------------------------------
    # CREATE SPARK SESSION
    # -----------------------------------------------------
    spark = SparkSession.builder.appName("BigData_Tarea1").master("local[*]").getOrCreate()
    # -----------------------------------------------------
    # Load data
    # -----------------------------------------------------
    cyclists_df = load_cyclists(spark, archivo_ciclistas)
    routes_df = load_routes(spark, archivo_rutas)
    activities_df = load_activities(spark, archivo_actividades)
    # Data joins
    # -----------------------------------------------------
    joined_df = join_data(cyclists_df, routes_df, activities_df)
    # -----------------------------------------------------
    # intermeediate agregations
    # -----------------------------------------------------
    totals_df = calculate_total_kilometers(joined_df)
    averages_df = calculate_daily_average(joined_df)
    # -----------------------------------------------------
    # Final results
    # -----------------------------------------------------
    top_total_km= get_top_cyclists_by_total_km(totals_df, TOP_N)
    top_daily_average = get_top_cyclists_by_daily_average(averages_df, TOP_N)
    # -----------------------------------------------------
    # Show results
    # -----------------------------------------------------
    print("\n===Top 5 ciclistas por total de kilómetros recorridos:===")
    top_total_km.show()
    print("\n===Top 5 ciclistas por promedio diario de kilómetros recorridos:===")
    top_daily_average.show()
    # -----------------------------------------------------
    # Stop Spark session
    # -----------------------------------------------------
    spark.stop()
# =========================================================
# Entry point
# =========================================================
if __name__ == "__main__":
    main()