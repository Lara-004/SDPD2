import os
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-21-openjdk-amd64"

conf = SparkConf()
conf.set("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")

spark = (SparkSession.builder
    .appName("Salida2")
    .master("local[*]")
    .config(conf=conf)
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

# Lectura batch desde Kafka (append mode: solo filas nuevas por micro-batch)
df = (spark.read.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "purchases")
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
    .selectExpr("CAST(key AS STRING) as usuario", "CAST(value AS STRING) as producto")
    .filter(F.col("producto").isin(["book", "alarm clock", "t-shirts"])))

df.show(truncate=False)

with open("salida2.txt", "w") as f:
    f.write("CONSULTA 2 - Filtrado productos (outputMode=append)\n")
    f.write("=" * 60 + "\n")
    f.write(df.toPandas().to_string(index=False))

print("salida2.txt creado.")
spark.stop()
