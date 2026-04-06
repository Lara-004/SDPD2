import pandas as pd
import tomllib
from airflow.sdk import dag, task
from datetime import datetime


@dag(
    dag_id="pipeline_limpieza_reviews",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    tags=["reviews", "limpieza", "sdpd2"]
)
def pipeline_limpieza_reviews():
    """
    DAG de limpieza de datos de reviews.
    En este caso primero leemos el csv raw, después aplicamos la limpieza
    y al final guardamos el resultado limpio en la ruta definida en config.toml.
    """

    @task
    def cargar_config():
        """
        Esta task lee el archivo config.toml para poder usar las rutas definidas ahí.
        """
        try:
            with open("config.toml", "rb") as f:
                config = tomllib.load(f)
            return config
        except FileNotFoundError:
            print("Error: no se encuentra el archivo config.toml")
            return None

    @task
    def limpieza_df(config):
        """
        Esta task carga el archivo raw y aplica las reglas de limpieza.
        Devuelve el dataframe limpio en formato diccionario para pasarlo a la siguiente task.
        """
        if config is None:
            return None

        # Leemos la ruta del archivo raw desde el TOML
        input_file = config["paths"]["raw_data"]
        print(f"Leyendo archivo para limpieza: {input_file}")

        # Leemos el csv
        df = pd.read_csv(input_file, low_memory=False)

        # Quitar duplicados por id de la review
        if "id" in df.columns:
            df = df.drop_duplicates(subset=["id"], keep="first")

        # Eliminar filas con valores nulos
        df = df.dropna()

        # Eliminar filas totalmente duplicadas
        df = df.drop_duplicates()

        # Limpiar espacios en blanco en nombres
        if "reviewer_name" in df.columns:
            df["reviewer_name"] = df["reviewer_name"].astype(str).str.strip()

        # Limpiar espacios y saltos de línea en comments
        if "comments" in df.columns:
            df["comments"] = (
                df["comments"]
                .astype(str)
                .str.replace(r"[\r\n]+", " ", regex=True)
                .str.replace(r"\s+", " ", regex=True)
                .str.strip()
            )

        # Convertir ids a string
        id_columns = ["listing_id", "id", "reviewer_id"]
        for col in id_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int).astype(str)

        # Convertir date a datetime
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")

        print("Limpieza completada con éxito.")

        # Lo devolvemos como diccionario para que Airflow lo pueda pasar a otra task
        return df.to_dict(orient="records")

    @task
    def guardar_limpieza_df(data, config):
        """
        Esta task recibe los datos ya limpios y los guarda en la ruta clean_data.
        """
        if data is None or config is None:
            print("No hay datos o configuración para guardar.")
            return None

        # Reconstruimos el dataframe
        df = pd.DataFrame(data)

        # Sacamos la ruta de salida del TOML
        output_path = config["paths"]["clean_data"]

        # Guardamos el csv limpio
        df.to_csv(output_path, index=False)

        print(f"Archivo limpio guardado en: {output_path}")
        return output_path

    # Orden de ejecución del DAG
    config = cargar_config()
    data_limpia = limpieza_df(config)
    guardar_limpieza_df(data_limpia, config)


pipeline_limpieza_reviews()
