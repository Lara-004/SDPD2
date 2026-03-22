import json
import pathlib
import csv
from kafka import KafkaProducer

def publish_to_kafka():
    # Definimos el archivo de entrada, que es el csv ya transformado
    input_file = pathlib.Path("reviews_transformed.csv")

    # Definimos el topic de Kafka donde vamos a publicar los mensajes
    topic_name = "reviews_clean"

    # Creamos el productor de Kafka
    # value_serializer sirve para convertir cada mensaje a formato JSON en bytes
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    # Abrimos el archivo transformado para leerlo fila por fila
    with input_file.open(mode="r", encoding="utf-8", newline="") as infile:
        reader = csv.DictReader(infile)

        # Recorremos cada fila del csv
        for row in reader:
            # Creamos el mensaje que queremos enviar a Kafka
            # Aquí seleccionamos las columnas más importantes
            message = {
                "review_id": row.get("id", ""),
                "listing_id": row.get("listing_id", ""),
                "reviewer_id": row.get("reviewer_id", ""),
                "reviewer_name": row.get("reviewer_name", ""),
                "date": row.get("date", ""),
                "comments_clean": row.get("comments_clean", ""),
                "comment_length": row.get("comment_length", ""),
                "word_count": row.get("word_count", ""),
                "review_year": row.get("review_year", ""),
                "review_month": row.get("review_month", "")
            }

            # Enviamos el mensaje al topic
            producer.send(topic_name, value=message)

    # Forzamos el envío de todos los mensajes pendientes
    producer.flush()

    # Cerramos el productor
    producer.close()

    print("Publicación en Kafka completada")


publish_to_kafka()
