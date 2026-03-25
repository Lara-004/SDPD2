import requests
import pathlib
import tomllib  # Si usas Python < 3.11, instala 'tomli' y cambia esto a import tomli as tomllib

def extraer_datos():
    """
    Tarea 1: Automatización de la descarga y almacenamiento de datos en crudo.
    Lee la configuración desde el archivo .toml y descarga el CSV desde GitHub.
    """
    
    # 1. Cargar la configuración del archivo TOML
    # El enunciado exige que las variables no estén 'hardcoded'
    try:
        with open("config.toml", "rb") as f:
            config = tomllib.load(f)
    except FileNotFoundError:
        print("Error: No se encuentra el archivo config.toml")
        return None

    url = config["paths"]["source_url"]
    destino = config["paths"]["raw_data"]

    print(f"Iniciando descarga desde: {url}")

    try:
        # 2. Realizar la petición GET para descargar el archivo
        response = requests.get(url, timeout=10)
        
        # Comprobar si la descarga fue exitosa (Código 200)
        response.raise_for_status() 

        # 3. Guardar el contenido en el archivo local
        # Usamos pathlib para asegurar compatibilidad de rutas (Windows/Linux)
        ruta_salida = pathlib.Path(destino)
        with open(ruta_salida, 'wb') as f:
            f.write(response.content)

        print(f"Archivo descargado correctamente y guardado en: {ruta_salida.absolute()}")
        return str(ruta_salida)

    except requests.exceptions.RequestException as e:
        print(f"Error durante la descarga: {e}")
        raise e

if __name__ == "__main__":
    # Esto permite probar el script de forma independiente en PyCharm
    extraer_datos()
