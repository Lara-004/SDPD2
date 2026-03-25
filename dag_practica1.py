import pandas as pd
import tomllib  # Requiere Python 3.11+. en caso de anterior 'tomli'
import pathlib
import json
import requests
from datetime import datetime
from airflow.decorators import dag, task
from kafka import KafkaProducer

#  Cargar configuración (.toml)
with open("config.toml", "rb") as f:
    config = tomllib.load(f)


@dag(
    dag_id="dag_practica1_airflow",
    schedule=None,
    start_date=datetime(2026, 3, 1),
    catchup=False,
    tags=['sdpd', 'practica1'],
)
def pipeline_estudiante():
    @task
    def extraer():
        """Tarea 1: Automatización de la descarga y almacenamiento """
        url = config["paths"]["source_url"]
        path = config["paths"]["raw_data"]

        response = requests.get(url)
        if response.status_code == 200:
            with open(path, 'wb') as f:
                f.write(response.content)
            return path
        else:
            raise Exception("Error en la descarga")

    @task
    def limpiar(input_file: str):
        """Tarea 2: Limpieza de datos (Duplicados y NAs) [cite: 32]"""
        df = pd.read_csv(input_file, low_memory=False)

        # 02_Limpieza.py
        df = df.drop_duplicates(subset=['id'], keep='first').dropna()

        if 'comments' in df.columns:
            df['comments'] = df['comments'].astype(str).str.replace(r'[\r\n]+', ' ', regex=True).str.strip()

        output = config["paths"]["clean_data"]
        df.to_csv(output, index=False)
        return output

    @task
    def transformar(input_file: str):
        """Tarea 3: Transformación de variables y fechas [cite: 35]"""
        df = pd.read_csv(input_file)

        # 03_transformacion.py
        df['comments_clean'] = df['comments'].fillna('').str.lower().str.replace(r'[^\w\s]', '', regex=True)
        df['comment_length'] = df['comments_clean'].str.len()

        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df['review_year'] = df['date'].dt.year

        output = config["paths"]["transformed_data"]
        df.to_csv(output, index=False)
        return output

    @task
    def cargar_kafka(input_file: str):
        """Tarea 4: Inserción en cola de mensajes Apache Kafka [cite: 50]"""
        producer = KafkaProducer(
            bootstrap_servers=config["kafka"]["bootstrap_servers"],
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

        df = pd.read_csv(input_file)
        # Enviamos los registros como JSON a Kafka
        for _, row in df.head(100).iterrows():  # head(100) para no saturar si es muy grande
            producer.send(config["kafka"]["topic"], value=row.to_dict())

        producer.flush()
        producer.close()
        return "Datos enviados a Kafka"

    # Secuencia de ejecución
    raw_csv = extraer()
    clean_csv = limpiar(raw_csv)
    trans_csv = transformar(clean_csv)
    cargar_kafka(trans_csv)



dag_practica = pipeline_estudiante()