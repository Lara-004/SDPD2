# Práctica 2 - Spark Structured Streaming
## Respuestas a las preguntas del apartado 3.2.1

---

### Pregunta 1
**¿Por qué no se obtiene ningún valor en la primera iteración que imprime por la terminal el resultado de la consulta a la tabla `raw_data`?**

La primera iteración aparece vacía porque el query de Structured Streaming se ejecuta de forma **asíncrona**. Al llamar a `.start()`, Spark lanza el streaming query en un hilo en segundo plano y devuelve el control al programa principal de inmediato, sin esperar a que el primer micro-batch haya sido procesado.

En esa primera iteración (ejecutada prácticamente a la vez que `.start()`), Spark todavía está realizando varias tareas internas: establecer la conexión con el broker Kafka, negociar los offsets con el topic, leer el micro-batch inicial y materializarlo en la tabla en memoria `raw_data`. Todo ese proceso lleva unos instantes, por lo que en el momento en que el `SELECT * FROM raw_data` se ejecuta la tabla aún no contiene ninguna fila.

Es el `sleep(3)` entre iteraciones el que da tiempo suficiente al micro-batch para completarse, razón por la que en la segunda o tercera iteración ya se obtienen resultados.

---

### Pregunta 2
**¿Qué función tiene la siguiente línea de código para configurar el input stream de Kafka?**
```python
.option("startingOffsets", "earliest")
```

Esta opción le indica a Spark desde qué posición del topic de Kafka debe comenzar a leer. Los valores posibles son:

- `"earliest"`: empieza desde el **offset más antiguo disponible** en el topic, es decir, lee todos los mensajes que existan desde el principio, incluyendo los producidos antes de que el stream arrancara.
- `"latest"` (valor por defecto): empieza desde el **offset más reciente**, lo que significa que solo se leen los mensajes que se publiquen **después** de que el stream se haya iniciado.

En el contexto de la práctica, usar `"earliest"` es fundamental para que Spark pueda consumir los mensajes que el productor ya había cargado previamente en el topic `purchases`. Si se usara `"latest"`, el stream arrancaría "vacío" y solo procesaría mensajes futuros.

---

### Pregunta 3
**¿Qué tipo de stream de salida crea el siguiente fragmento de código? ¿Por qué podemos hacer consultas directamente sobre dicho stream como si fuese una tabla de datos ordinaria?**

```python
describe_query = (input_data.writeStream.queryName("raw_data")
                  .format("memory").outputMode("append")
                  .start())
```

Este fragmento crea un **stream de salida en memoria** (*memory sink*). Los tres elementos clave son:

- `.format("memory")`: especifica que el sink de destino es la **memoria RAM de la JVM** de Spark, en lugar de un sistema externo (fichero, base de datos, Kafka, etc.). Los datos procesados se almacenan en una estructura interna del SparkSession.

- `.queryName("raw_data")`: asigna un nombre a la query y, al mismo tiempo, **registra automáticamente una tabla temporal** con ese nombre en el catálogo del SparkSession. Esta tabla es accesible mediante `spark.sql(...)` o la API de DataFrames.

- `.outputMode("append")`: cada micro-batch añade únicamente las filas nuevas a la tabla en memoria, sin borrar las anteriores (a diferencia de `complete`, que reescribe la tabla entera).

Podemos hacer consultas SQL directamente sobre `raw_data` porque Spark, al usar el memory sink con un `queryName`, **registra el stream como una vista temporal** en el catálogo interno del SparkSession. Eso hace que sea indistinguible de una tabla estática desde el punto de vista del usuario: `spark.sql("SELECT * FROM raw_data")` lee el estado acumulado de los micro-batches hasta ese momento, exactamente igual que si fuera una tabla normal cargada con `spark.read`.

Esta capacidad es una de las principales ventajas del modelo de Spark Structured Streaming: la **unificación de la API** para datos en batch y en streaming, permitiendo al usuario razonar sobre el stream como si fuera una tabla en constante crecimiento (*unbounded table*).
