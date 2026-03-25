import pandas as pd
import tomllib
import pathlib

def limpieza_df():
    """
    Tarea 2: Limpieza de datos.
    Carga el archivo raw definido en el TOML, aplica reglas de limpieza y retorna el DataFrame.
    """
    # 1. Cargar configuración desde el archivo TOML
    try:
        with open("config.toml", "rb") as f:
            config = tomllib.load(f)
    except FileNotFoundError:
        print("Error: No se encuentra el archivo config.toml")
        return None

    # 2. Leer el archivo descargado en la etapa anterior
    input_file = config["paths"]["raw_data"]
    print(f"Leyendo archivo para limpieza: {input_file}")
    
    # low_memory=False evita avisos de tipos de datos mixtos
    df = pd.read_csv(input_file, low_memory=False)

    # --- LÓGICA DE LIMPIEZA ---
    
    # Quitar duplicados por id (identificador único de la review)
    df = df.drop_duplicates(subset=['id'], keep='first')

    # Eliminar filas con cualquier valor nulo (NA) para asegurar calidad
    df = df.dropna()

    # Eliminar filas que sean completamente idénticas
    df = df.drop_duplicates()

    # Limpiar espacios en blanco en nombres
    if 'reviewer_name' in df.columns:
        df['reviewer_name'] = df['reviewer_name'].astype(str).str.strip()

    # Limpiar la columna de comentarios
    if 'comments' in df.columns:
        df['comments'] = (
            df['comments']
            .astype(str)
            .str.replace(r'[\r\n]+', ' ', regex=True)  # Sustituir saltos de línea por espacios
            .str.replace(r'\s+', ' ', regex=True)      # Eliminar espacios múltiples
            .str.strip()                               # Quitar espacios al inicio y final
        )

    # Convertir IDs a string para evitar errores de precisión (overflow) con números largos
    id_columns = ['listing_id', 'id', 'reviewer_id']
    for col in id_columns:
        if col in df.columns:
            # Convertimos a numérico, rellenamos errores y pasamos a int antes de string
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int).astype(str)

    # Convertir la columna date a objeto datetime real
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')

    print("Limpieza completada con éxito.")
    return df

def guardar_limpieza_df(df):
    """
    Guarda el DataFrame limpio en la ruta especificada en el archivo TOML.
    """
    with open("config.toml", "rb") as f:
        config = tomllib.load(f)
    
    output_path = config["paths"]["clean_data"]
    df.to_csv(output_path, index=False)
    print(f"Archivo limpio guardado en: {output_path}")
    return output_path

if __name__ == "__main__":
    # Prueba rápida independiente
    dataframe_limpio = limpieza_df()
    if dataframe_limpio is not None:
        guardar_limpieza_df(dataframe_limpio)
