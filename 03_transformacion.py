import pandas as pd
import tomllib
import pathlib

def get_transformed_df():
    """
    Tarea 3: Transformación de datos.
    Carga el archivo limpio, genera nuevas métricas y variables temporales.
    """
    # 1. Cargar configuración desde el TOML
    try:
        with open("config.toml", "rb") as f:
            config = tomllib.load(f)
    except FileNotFoundError:
        print("Error: No se encuentra el archivo config.toml")
        return None

    # 2. Leer el archivo que viene de la fase de limpieza
    input_file = config["paths"]["clean_data"]
    print(f"Leyendo archivo para transformación: {input_file}")
    df = pd.read_csv(input_file)

    # --- LÓGICA DE TRANSFORMACIÓN ---

    # Aseguramos que comments sea texto y rellenamos vacíos
    df['comments'] = df['comments'].fillna('').astype(str)

    # 1. Crear 'comments_clean': minúsculas, sin puntuación y sin espacios extra
    df['comments_clean'] = (
        df['comments']
        .str.lower()
        .str.replace(r'\s+', ' ', regex=True)
        .str.replace(r'[^\w\s]', '', regex=True)
        .str.strip()
    )

    # 2. Variable métrica: Longitud del comentario (caracteres)
    df['comment_length'] = df['comments_clean'].str.len()

    # 3. Variable métrica: Conteo de palabras
    df['word_count'] = df['comments_clean'].str.split().str.len()

    # 4. Transformaciones temporales (Año y Mes)
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df['review_year'] = df['date'].dt.year
        df['review_month'] = df['date'].dt.month
        # Rellenamos NAs en fechas si aparecieran tras la conversión
        df['review_year'] = df['review_year'].fillna(0).astype(int)
        df['review_month'] = df['review_month'].fillna(0).astype(int)

    print("Transformación completada con éxito.")
    return df

def save_transformed_df(df):
    """
    Guarda el DataFrame transformado en la ruta final definida en el TOML.
    """
    with open("config.toml", "rb") as f:
        config = tomllib.load(f)
    
    output_path = config["paths"]["transformed_data"]
    df.to_csv(output_path, index=False)
    print(f"Archivo transformado guardado en: {output_path}")
    return output_path

if __name__ == "__main__":
    # Prueba rápida independiente
    df_trans = get_transformed_df()
    if df_trans is not None:
        save_transformed_df(df_trans)
