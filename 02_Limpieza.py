import pandas as pd

def limpieza_df(input_file='reviews.csv'):
    df = pd.read_csv(input_file, low_memory=False)

    # Quitar duplicados por id
    df = df.drop_duplicates(subset=['id'], keep='first')

    # Eliminar filas con NA
    df = df.dropna()

    # Eliminar filas que sean completamente idénticas
    df = df.drop_duplicates()

    # Limpiar espacios en texto
    if 'reviewer_name' in df.columns:
        df['reviewer_name'] = df['reviewer_name'].astype(str).str.strip()

    if 'comments' in df.columns:
        df['comments'] = (
            df['comments']
            .astype(str)
            .str.replace(r'[\r\n]+', ' ', regex=True)  # Quitar saltos de línea
            .str.replace(r'\s+', ' ', regex=True)  # Quitar espacios múltiples
            .str.strip()
        )

    # Convertir IDs a character (string) para evitar overflow
    id_columns = ['listing_id', 'id', 'reviewer_id']
    for col in id_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int).astype(str)

    # Convertir fecha a objeto datetime de Python
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')

    return df


def guardar_limpieza_df(output_name='reviews_clean.csv'):
    # Ejecutamos la limpieza
    df_limpio = limpieza_df()

    # Guardamos el resultado
    df_limpio.to_csv(output_name, index=False)

    print(f"Limpieza completada. Registros finales: {len(df_limpio)}")
    print(f"Columnas resultantes: {df_limpio.columns.tolist()}")


if __name__ == "__main__":
    guardar_limpieza_df()