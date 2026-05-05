#!/usr/bin/env python
"""
Spark Structured Streaming - Consumidor Kafka - Grupo 2
Práctica 2 - SDPD2 (Sistemas Distribuidos de Procesamiento de Datos II)

Lee el flujo de datos del topic 'purchases' desde el broker Kafka del Grupo 2
usando PySpark 4.1.1 en modo local. Realiza tres consultas sobre el stream:

  · raw_data   : volcado directo de mensajes (outputMode=append)   → [consola]
  · agg_data   : conteo de compras por producto (outputMode=complete) → salida1.txt
  · filtered_data: filtrado de mensajes (outputMode=append)         → salida2.txt

Requisitos:
    - Entorno virtual pyspark-411 activo (uv pip install pyspark[connect]==4.1.1)
    - Java 17+ instalado y JAVA_HOME configurado
    - Broker Kafka accesible

Uso:
    python struct_kafka_consumer_local_g2.py
"""

import os
from time import sleep

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ─── Configuración del entorno ────────────────────────────────────────────────

# Ruta de Java 17 (ajustar si difiere en el equipo)
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-21-openjdk-amd64"

# Dirección del broker Kafka del Grupo 2
BOOTSTRAP_SERVERS_LOCAL = "localhost:9092"
BOOTSTRAP_SERVERS_LAB   = "localhost:9092"  # Grupo 2
KAFKA_BROKER = BOOTSTRAP_SERVERS_LAB

TOPIC = "purchases"

# Conector Kafka para Spark 4.1.1 (Scala 2.13)
# IMPORTANTE: Spark 4.x usa Scala 2.13; versiones anteriores usaban 2.12
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"

# Archivos de salida para las dos consultas
OUTPUT_FILE_1 = "salida1.txt"   # Consulta 1: aggregation (complete mode)
OUTPUT_FILE_2 = "salida2.txt"   # Consulta 2: filter      (append mode)

# Número de iteraciones de monitorización del stream principal
MONITOR_ITERS  = 5
SLEEP_SECS     = 3


# ─── Inicialización de Spark ──────────────────────────────────────────────────

conf = SparkConf()
# Carga el conector Spark-Kafka como dependencia Maven al iniciar la sesión
conf.set("spark.jars.packages", KAFKA_PACKAGE)

spark = (SparkSession.builder
    .appName("StructuredStreamingKafka_Grupo2")
    .master("local[*]")          # Modo local con todos los cores disponibles
    .config(conf=conf)
    .getOrCreate())

# Reducimos el nivel de log para que la salida sea más legible
spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print(" Spark Structured Streaming - Grupo 2")
print(f" Broker : {KAFKA_BROKER}")
print(f" Topic  : {TOPIC}")
print("=" * 60)


# ─── Lectura del stream desde Kafka ──────────────────────────────────────────

input_data = (spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", TOPIC)
    # "earliest": empieza a leer desde el mensaje más antiguo disponible en el topic.
    # Si se usara "latest" (valor por defecto), solo se leerían mensajes nuevos.
    .option("startingOffsets", "earliest")
    .load()
    # Casteamos el campo 'value' a STRING; descartamos los metadatos de Kafka
    .selectExpr("CAST(value AS STRING)")
)


# ─── Stream principal: raw_data (outputMode=append) ──────────────────────────
# Volcamos el stream en memoria bajo el nombre "raw_data" para poder consultarlo
# con SQL estándar, como si fuera una tabla estática de la sesión de Spark.
# outputMode="append" añade cada micro-batch a la tabla sin reemplazar los anteriores.

describe_query = (input_data.writeStream
    .queryName("raw_data")
    .format("memory")
    .outputMode("append")
    .start())

print("\n[raw_data] Stream iniciado. Monitorizando durante "
      f"{MONITOR_ITERS} iteraciones...\n")

for i in range(MONITOR_ITERS):
    print(f"--- Iteración {i + 1} / {MONITOR_ITERS} ---")
    spark.sql("SELECT * FROM raw_data").show(truncate=False)
    sleep(SLEEP_SECS)


# ─── CONSULTA 1: Conteo por producto (outputMode=complete) ───────────────────
#
# 'complete' es el modo de salida adecuado para consultas de agregación:
# en cada micro-batch se reescribe la tabla completa con los totales actualizados.
# No se puede usar 'append' con agregaciones estáfull porque Spark no puede garantizar
# que el resultado anterior sea correcto al llegar nuevos datos.

print("\n[agg_data] Iniciando consulta de agregación (complete mode)...\n")

agg_data = (input_data
    .groupBy("value")                   # Agrupamos por nombre de producto
    .agg(F.count("*").alias("total"))   # Contamos cuántas veces aparece cada uno
)

agg_query = (agg_data.writeStream
    .queryName("agg_data")
    .format("memory")
    .outputMode("complete")   # Resultado completo en cada micro-batch
    .start())

sleep(SLEEP_SECS * 2)   # Esperamos a que el micro-batch procese datos

result1 = spark.sql("SELECT * FROM agg_data ORDER BY total DESC")
print("[Consulta 1] Compras por producto (todos los micro-batches acumulados):")
result1.show(truncate=False)

# Volcamos el resultado a salida1.txt
with open(OUTPUT_FILE_1, "w", encoding="utf-8") as f:
    f.write("CONSULTA 1 - Conteo de compras por producto (outputMode=complete)\n")
    f.write("=" * 60 + "\n")
    f.write(result1.toPandas().to_string(index=False))
    f.write("\n")

print(f"[Consulta 1] Resultado guardado en '{OUTPUT_FILE_1}'")


# ─── CONSULTA 2: Filtrado de productos específicos (outputMode=append) ────────
#
# 'append' es adecuado aquí porque la consulta es sin estado (stateless):
# cada nuevo mensaje se evalúa de forma independiente; no necesitamos
# recordar el estado anterior para calcular el resultado.
# Solo se añaden al stream de salida las filas que cumplan el filtro.

print("\n[filtered_data] Iniciando consulta de filtrado (append mode)...\n")

filtered_data = (input_data
    .filter(
        F.col("value").isin(["book", "alarm clock"])  # Solo ciertos productos
    )
    .withColumn("label", F.lit("high_value_item"))    # Etiqueta auxiliar
)

filtered_query = (filtered_data.writeStream
    .queryName("filtered_data")
    .format("memory")
    .outputMode("append")   # Solo las filas nuevas de cada micro-batch
    .start())

sleep(SLEEP_SECS * 2)

result2 = spark.sql("SELECT * FROM filtered_data")
print("[Consulta 2] Mensajes filtrados (book y alarm clock):")
result2.show(truncate=False)

# Volcamos el resultado a salida2.txt
with open(OUTPUT_FILE_2, "w", encoding="utf-8") as f:
    f.write("CONSULTA 2 - Filtrado de productos de alto valor (outputMode=append)\n")
    f.write("=" * 60 + "\n")
    f.write(result2.toPandas().to_string(index=False))
    f.write("\n")

print(f"[Consulta 2] Resultado guardado en '{OUTPUT_FILE_2}'")


# ─── Cierre ordenado ──────────────────────────────────────────────────────────

print("\nCerrando streams y sesión de Spark...")
agg_query.stop()
filtered_query.stop()
describe_query.stop()
spark.stop()
print("Fin del programa.")
