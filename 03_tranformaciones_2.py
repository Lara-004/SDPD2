import pandas as pd
import tomllib
from airflow.sdk import dag, task
from datetime import datetime


@dag(
    dag_id="pipeline_transformacion_reviews",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    tags=["reviews", "transformacion", "sdpd2"]
)
def pipeline_transformacion_reviews():
    """
    DAG de transformación de datos.
    Aquí se lee el archivo limpio, se crean nuevas variables
    y al final se guarda el resultado transformado.
    """

    @task
    def cargar_config():
        """
        Esta task lee el archivo config.toml para usar las rutas definidas ahí.
        """
        try:
            with open("config.toml", "rb") as f:
                config = tomllib.load(f)
            return config
        except FileNotFoundError:
            print("Error: No se encuentra el archivo config.toml")
            return None

    @task
    def get_transformed_df(config):
        """
        Esta task lee el archivo limpio y aplica la transformación de datos.
        Devuelve el resultado en formato diccionario para poder pasarlo a la siguiente task.
        """
        if config is None:
            return None

        # Leemos la ruta del archivo limpio desde el TOML
        input_file = config["paths"]["clean_data"]
        print(f"Leyendo archivo para transformación: {input_file}")

        # Leemos el csv limpio
        df = pd.read_csv(input_file)

        # Aseguramos que la columna comments sea texto
        # y sustituimos valores vacíos por cadena vacía
        if "comments" in df.columns:
            df["comments"] = df["comments"].fillna("").astype(str)

            # Creamos una nueva columna con el comentario más limpio
            # Lo pasamos a minúsculas, quitamos espacios repetidos
            # y eliminamos signos de puntuación
            df["comments_clean"] = (
                df["comments"]
                .str.lower()
                .str.replace(r"\s+", " ", regex=True)
                .str.replace(r"[^\w\s]", "", regex=True)
                .str.strip()
            )

            # Creamos una variable con la longitud del comentario
            df["comment_length"] = df["comments_clean"].str.len()

            # Creamos otra variable con el número de palabras
            df["word_count"] = df["comments_clean"].str.split().str.len()

        # Si existe la fecha, sacamos el año y el mes
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df["review_year"] = df["date"].dt.year
            df["review_month"] = df["date"].dt.month

            # Si alguna fecha no se pudo convertir, rellenamos con 0
            df["review_year"] = df["review_year"].fillna(0).astype(int)
            df["review_month"] = df["review_month"].fillna(0).astype(int)

        print("Transformación completada con éxito.")

        # Convertimos el dataframe a diccionario para pasarlo entre tasks
        return df.to_dict(orient="records")

    @task
    def save_transformed_df(data, config):
        """
        Esta task recibe los datos transformados y los guarda
        en la ruta final definida en el archivo TOML.
        """
        if data is None or config is None:
            print("No hay datos o configuración para guardar.")
            return None

        # Reconstruimos el dataframe
        df = pd.DataFrame(data)

        # Leemos la ruta de salida desde el TOML
        output_path = config["paths"]["transformed_data"]

        # Guardamos el archivo transformado
        df.to_csv(output_path, index=False)

        print(f"Archivo transformado guardado en: {output_path}")
        return output_path

    # Orden de ejecución del DAG
    config = cargar_config()
    data_transformada = get_transformed_df(config)
    save_transformed_df(data_transformada, config)


pipeline_transformacion_reviews()
