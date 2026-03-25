import pendulum
from airflow.decorators import dag, task
from datetime import datetime

# Importamos las funciones de tus archivos corregidos
# Asegúrate de que los nombres de los archivos coincidan en tu carpeta
import etapa_01_extraccion as e1
import etapa_02_limpieza as e2
import etapa_03_transformacion as e3
import etapa_04_kafka as e4

@dag(
    dag_id="pipeline_airbnb_reviews_final",
    schedule=None,  # Se ejecuta manualmente
    start_date=pendulum.datetime(2026, 3, 1, tz="UTC"),
    catchup=False,
    tags=['sdpd', 'practica1'],
)
def pipeline_practica():

    @task
    def extraer():
        """Llama al Bloque 01"""
        return e1.extraer_datos()

    @task
    def limpiar(path_raw):
        """Llama al Bloque 02"""
        df_limpio = e2.limpieza_df()
        return e2.guardar_limpieza_df(df_limpio)

    @task
    def transformar(path_clean):
        """Llama al Bloque 03"""
        df_trans = e3.get_transformed_df()
        return e3.save_transformed_df(df_trans)

    @task
    def cargar_kafka(path_final):
        """Llama al Bloque 04"""
        e4.publish_to_kafka()
        return "Flujo completado con éxito"

    # --- DEFINICIÓN DEL FLUJO (LA SECUENCIA) ---
    raw_csv = extraer()
    clean_csv = limpiar(raw_csv)
    trans_csv = transformar(clean_csv)
    cargar_kafka(trans_csv)

# Instanciar el DAG
dag_final = pipeline_practica()
