import requests
import pathlib
import tomllib

from datetime import datetime
from airflow.decorators import dag, task


@task
def extraer_datos():
    """
    Tarea 1: descarga y guardado de datos en crudo.
    """

    try:
        with open("config.toml", "rb") as f:
            config = tomllib.load(f)
    except FileNotFoundError:
        raise FileNotFoundError("No se encuentra el archivo config.toml")

    url = config["paths"]["source_url"]
    destino = config["paths"]["raw_data"]

    print(f"Iniciando descarga desde: {url}")

    response = requests.get(url, timeout=10)
    response.raise_for_status()

    ruta_salida = pathlib.Path(destino)
    with open(ruta_salida, "wb") as f:
        f.write(response.content)

    print(f"Archivo descargado correctamente en: {ruta_salida.absolute()}")
    return str(ruta_salida)


@task
def transformar_datos(ruta_fichero):
    """
    Tarea 2: aquí haríais la limpieza o transformación.
    """
    print(f"Transformando datos desde: {ruta_fichero}")

    # aquí iría vuestra limpieza real
    ruta_transformada = ruta_fichero.replace(".csv", "_clean.csv")

    return ruta_transformada


@task
def cargar_resultado(ruta_final):
    """
    Tarea 3: guardar resultado final o mandarlo a Kafka.
    """
    print(f"Resultado final preparado en: {ruta_final}")
    # aquí iría la inserción en Kafka o almacenamiento final


@dag(
    dag_id="pipeline_extraccion_datos",
    start_date=datetime(2026, 3, 20),
    schedule=None,
    catchup=False,
    tags=["practica", "etl"]
)
def mi_pipeline():
    ruta_raw = extraer_datos()
    ruta_clean = transformar_datos(ruta_raw)
    cargar_resultado(ruta_clean)


dag = mi_pipeline()
