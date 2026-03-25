import json
import pathlib
import csv
import tomllib
from kafka import KafkaProducer

def publish_to_kafka():
    """
    Tarea 4: Carga de resultados en Apache Kafka.
    Lee los datos transformados y los envía al broker de Kafka definido en el TOML.
    """
    # 1. Cargar configuración desde el TOML
    try:
        with open("config.toml", "rb") as f:
            config = tomllib.load(f)
    except FileNotFoundError:
        print("Error: No se encuentra el archivo config.toml")
        return None

    # 2. Definir rutas y parámetros de Kafka desde el TOML
    input_file = pathlib.Path(config["paths"]["transformed_data"])
    topic_name = config["kafka"]["topic"]
    bootstrap_servers = config["kafka"]["bootstrap_servers"]

    print(f"Conectando a Kafka en: {bootstrap_servers}...")

    # 3. Crear el productor de Kafka
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

        # 4. Leer el archivo transformado y enviar mensaje por mensaje
        if not input_file.exists():
            print(f"Error: El archivo {input_file} no existe.")
            return

        with input_file.open(mode="r", encoding="utf-8", newline="") as infile:
            reader = csv.DictReader(infile)
            count = 0
            for row in reader:
                # Enviamos la fila completa como un diccionario JSON
                producer.send(topic_name, value=row)
                count += 1
            
            # Forzamos el envío de mensajes pendientes
            producer.flush()
            print(f"Éxito: Se han enviado {count} mensajes al topic '{topic_name}'.")

    except Exception as e:
        print(f"Error al conectar o enviar a Kafka: {e}")
    finally:
        if 'producer' in locals():
            producer.close()

if __name__ == "__main__":
    # Prueba rápida independiente
    publish_to_kafka()
