import requests
import pathlib
import tomllib

from datetime import datetime
from airflow import DAG
from airflow.decorators import dag, task


@task
def extraer_datos():
    """
    Tarea 1: descarga los datos en crudo.
    """

    try:
        with open("/home/vboxuser/airflow/dags/config.toml", "rb") as f:
            config = tomllib.load(f)
    except FileNotFoundError:
        raise FileNotFoundError("No se encuentra el archivo config.toml")

    url = config["paths"]["source_url"]
    destino = config["paths"]["raw_data"]

    print(f"Iniciando descarga desde: {url}")

    response = requests.get(url, timeout=20)
    response.raise_for_status()

    ruta_salida = pathlib.Path(destino)
    ruta_salida.parent.mkdir(parents=True, exist_ok=True)

    with open(ruta_salida, "wb") as f:
        f.write(response.content)

    print(f"Archivo descargado correctamente en: {ruta_salida}")
    return str(ruta_salida)


@task
def transformar_datos(ruta_fichero):
    """
    Tarea 2: prepara una ruta transformada.
    Aquí luego podéis meter la limpieza real.
    """

    print(f"Transformando datos desde: {ruta_fichero}")

    ruta_original = pathlib.Path(ruta_fichero)
    ruta_transformada = ruta_original.with_name(ruta_original.stem + "_clean" + ruta_original.suffix)

    print(f"Ruta transformada generada: {ruta_transformada}")
    return str(ruta_transformada)


@task
def cargar_resultado(ruta_final):
    """
    Tarea 3: deja preparado el resultado final.
    """

    print(f"Resultado final preparado en: {ruta_final}")
    print("Aquí iría la carga final a Kafka o al destino que os pidan.")


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
