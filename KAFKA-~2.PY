#!/usr/bin/env python
"""
Kafka Consumer - Grupo 2
Práctica 2 - SDPD2 (Sistemas Distribuidos de Procesamiento de Datos II)

Consume mensajes del topic 'purchases' desde el broker Kafka del Grupo 2
y los imprime por consola. Ejecutar con el entorno virtual 'kafka' activado.

Uso:
    python kafka-consumer-confluent_g2.py
    (Pulsar Ctrl+C para detener)
"""

from confluent_kafka import Consumer


# ─── Configuración del broker ─────────────────────────────────────────────────
# Cambiar a la dirección del servidor de laboratorio cuando se trabaje en los PCs EIF
BOOTSTRAP_SERVERS_LOCAL = "localhost:9092"
BOOTSTRAP_SERVERS_LAB   = "docker02.aulas.eif.urjc.es:9094"  # Grupo 2

config = {
    "bootstrap.servers":  BOOTSTRAP_SERVERS_LAB,
    "group.id":           "kafka-grupo2-consumer",  # ID de grupo del consumidor
    "enable.auto.commit": "false",                  # Commit manual para mayor control
    "auto.offset.reset":  "earliest"                # Leemos desde el principio del topic
}

TOPIC = "purchases"


if __name__ == "__main__":
    # Creamos el consumidor y nos suscribimos al topic
    consumer = Consumer(config)
    consumer.subscribe([TOPIC])
    print(f"Suscrito al topic '{TOPIC}'. Esperando mensajes... (Ctrl+C para salir)\n")

    try:
        while True:
            # poll() espera hasta 1 segundo por nuevos mensajes
            msg = consumer.poll(1.0)

            if msg is None:
                # Sin mensajes nuevos: puede tardar hasta session.timeout.ms
                # en el rebalanceo inicial del grupo consumidor
                print("Waiting...")

            elif msg.error():
                print(f"ERROR: {msg.error()}")

            else:
                # Imprimimos clave y valor del mensaje recibido
                print(
                    f"Consumed event from topic {msg.topic()}: "
                    f"key = {msg.key().decode('utf-8'):12} "
                    f"value = {msg.value().decode('utf-8'):12}"
                )

    except KeyboardInterrupt:
        print("\nInterrumpido por el usuario. Cerrando consumidor...")
        consumer.unsubscribe()

    finally:
        # Liberamos el offset y salimos del grupo de consumidores limpiamente
        consumer.close()
        print("Consumidor cerrado correctamente.")
