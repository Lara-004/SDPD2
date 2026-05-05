#!/usr/bin/env python
"""
Kafka Producer - Grupo 2
Práctica 2 - SDPD2 (Sistemas Distribuidos de Procesamiento de Datos II)

Produce mensajes aleatorios de compras al topic 'purchases' en el broker
Kafka del Grupo 2. Ejecutar con el entorno virtual 'kafka' activado.

Uso:
    python kafka-producer-confluent_g2.py
"""

from random import choice
from confluent_kafka import Producer


# ─── Configuración del broker ─────────────────────────────────────────────────
# Cambiar a la dirección del servidor de laboratorio cuando se trabaje en los PCs EIF
BOOTSTRAP_SERVERS_LOCAL = "localhost:9092"
BOOTSTRAP_SERVERS_LAB   = "docker02.aulas.eif.urjc.es:9094"  # Grupo 2

config = {
    "bootstrap.servers": BOOTSTRAP_SERVERS_LAB,
    "acks": "all"   # Garantía de entrega: espera confirmación de todos los brokers
}

# ─── Datos de ejemplo ─────────────────────────────────────────────────────────
TOPIC     = "purchases"
USER_IDS  = ["eabara-2", "jsmith-2", "sgarcia-2", "jbernard-2", "htanaka-2", "awalther-2"]
PRODUCTS  = ["book", "alarm clock", "t-shirts", "gift card", "batteries"]
NUM_MSGS  = 10  # Mensajes por ejecución


def delivery_callback(err, msg):
    """Callback que confirma (o reporta el fallo de) cada mensaje enviado."""
    if err:
        print(f"ERROR: Message failed delivery: {err}")
    else:
        print(
            f"Produced event to topic {msg.topic()}: "
            f"key = {msg.key().decode('utf-8'):12} "
            f"value = {msg.value().decode('utf-8'):12}"
        )


if __name__ == "__main__":
    # Creamos el productor con la configuración del broker
    producer = Producer(config)

    count = 0
    try:
        for _ in range(NUM_MSGS):
            user_id = choice(USER_IDS)
            product = choice(PRODUCTS)
            # Publicamos el producto como valor y el user_id como clave del mensaje
            producer.produce(TOPIC, product, user_id, callback=delivery_callback)
            count += 1
    except KeyboardInterrupt:
        print("\nInterrumpido por el usuario.")
    finally:
        # poll() activa los callbacks pendientes; flush() espera a que se envíen todos
        producer.poll(10000)
        producer.flush()
        print(f"\nTotal de mensajes enviados: {count}")
