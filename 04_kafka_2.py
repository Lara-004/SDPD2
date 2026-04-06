import json
import pathlib
import csv
import tomllib
from airflow.sdk import dag, task
from datetime import datetime
from kafka import KafkaProducer


@dag(
    dag_id="pipeline_kafka_reviews",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    tags=["reviews", "kafka", "sdpd2"]
)
def pipeline_kafka_reviews():
    """
    DAG de publicación en Kafka.
    Aquí se lee el archivo transformado y se envía fila por fila
    al topic de Kafka definido en el archivo config.toml.
    """

    @task
    def cargar_config():
        """
        Esta task lee el archivo config.toml para obtener
        las rutas y la configuración de Kafka.
        """
        try:
            with open("config.toml", "rb") as f:
                config = tomllib.load(f)
            return config
        except FileNotFoundError:
            print("Error: No se encuentra el archivo config.toml")
            return None

    @task
    def publish_to_kafka(config):
        """
        Esta task lee el archivo transformado y envía cada fila
        como mensaje JSON al topic de Kafka.
        """
        if config is None:
            print("No se pudo cargar la configuración.")
            return None

        # Leemos rutas y parámetros desde el TOML
        input_file = pathlib.Path(config["paths"]["transformed_data"])
        topic_name = config["kafka"]["topic"]
        bootstrap_servers = config["kafka"]["bootstrap_servers"]

        print(f"Conectando a Kafka en: {bootstrap_servers}...")

        # Comprobamos que el archivo existe antes de seguir
        if not input_file.exists():
            print(f"Error: El archivo {input_file} no existe.")
            return None

        try:
            # Creamos el productor de Kafka
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )

            # Leemos el csv transformado y enviamos fila por fila
            with input_file.open(mode="r", encoding="utf-8", newline="") as infile:
                reader = csv.DictReader(infile)
                count = 0

                for row in reader:
                    # Enviamos la fila completa como un mensaje JSON
                    producer.send(topic_name, value=row)
                    count += 1

            # Forzamos el envío de todos los mensajes pendientes
            producer.flush()

            print(f"Éxito: Se han enviado {count} mensajes al topic '{topic_name}'.")
            return count

        except Exception as e:
            print(f"Error al conectar o enviar a Kafka: {e}")
            return None

        finally:
            if "producer" in locals():
                producer.close()

    # Orden de ejecución
    config = cargar_config()
    publish_to_kafka(config)


pipeline_kafka_reviews()
