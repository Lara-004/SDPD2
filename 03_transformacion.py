import pandas as pd

def get_transformed_df():
    # Leemos el archivo que ya ha pasado por la fase de limpieza
    df = pd.read_csv('reviews_clean.csv')

    # Nos aseguramos de que la columna comments sea texto
    # Si hay valores vacíos, los sustituimos por una cadena vacía
    df['comments'] = df['comments'].fillna('').astype(str)

    # Creamos una nueva columna con el comentario más limpio y homogéneo
    # Lo pasamos a minúsculas, quitamos espacios repetidos y eliminamos signos de puntuación
    df['comments_clean'] = (
        df['comments']
        .str.lower()
        .str.replace(r'\s+', ' ', regex=True)
        .str.replace(r'[^\w\s]', '', regex=True)
        .str.strip()
    )

    # Creamos una columna con la longitud del comentario
    df['comment_length'] = df['comments_clean'].str.len()

    # Creamos otra columna con el número de palabras
    df['word_count'] = df['comments_clean'].str.split().str.len()

    # Transformamos la fecha y sacamos el año y el mes
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df['review_year'] = df['date'].dt.year
        df['review_month'] = df['date'].dt.month

    # Devolvemos el dataframe transformado
    return df


def save_transformed_df():
    # Guardamos el dataframe transformado en un nuevo csv
    df = get_transformed_df()
    df.to_csv('reviews_transformed.csv', index=False)
