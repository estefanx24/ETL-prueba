import boto3
import pandas as pd
from sqlalchemy import create_engine
import os

# Clase encargada de descargar archivos desde S3
class S3Downloader:
    def __init__(self, s3_bucket_path):
        self.s3_bucket_path = s3_bucket_path  # Ruta del bucket de S3
        self.s3 = boto3.client('s3')  # Cliente S3 para interactuar con el servicio de AWS

    # Método principal que descarga los archivos del bucket S3 a un directorio local
    def download_files(self, local_path):
        try:
            # Parseamos la ruta del bucket S3 para obtener el nombre del bucket y el prefijo
            bucket_name, prefix = self._parse_s3_path(self.s3_bucket_path)
            
            # Obtenemos la lista de archivos en el bucket S3 con el prefijo especificado
            files = self._list_s3_files(bucket_name, prefix)

            # Si no se encuentran archivos, mostramos un mensaje y retornamos una lista vacía
            if not files:
                print(f"No se encontraron archivos en la ruta S3: {self.s3_bucket_path}")
                return []

            # Descargamos los archivos desde S3 a la ruta local
            self._download_files_from_s3(bucket_name, files, local_path)
            return files  # Retorna los archivos descargados
        except Exception as e:
            print(f"Error al descargar los archivos desde S3: {e}")
            return []

    # Método privado para parsear la ruta S3 y separar el nombre del bucket y el prefijo
    def _parse_s3_path(self, s3_path):
        parts = s3_path.split('/')  # Separa la ruta S3 en partes
        bucket_name = parts[2]  # El nombre del bucket está en la tercera posición
        prefix = '/'.join(parts[3:])  # El prefijo es lo que viene después del nombre del bucket
        return bucket_name, prefix

    # Método privado que lista los archivos en un bucket S3 con el prefijo dado
    def _list_s3_files(self, bucket_name, prefix):
        response = self.s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)  # Lista los objetos en el bucket
        # Retorna las claves de los archivos encontrados
        return [content['Key'] for content in response.get('Contents', [])]

    # Método privado que descarga los archivos desde S3 a la ruta local
    def _download_files_from_s3(self, bucket_name, files, local_path):
        for file_key in files:
            # Crea la ruta local donde se descargará el archivo
            local_file_path = os.path.join(local_path, file_key.split('/')[-1])
            # Descarga el archivo de S3 a la ruta local
            self.s3.download_file(bucket_name, file_key, local_file_path)
            print(f"Archivo descargado: {local_file_path}")


# Clase encargada de insertar un DataFrame en MySQL
class MySQLInserter:
    def __init__(self, host, port, user, password, db_name):
        # Crea una cadena de conexión a MySQL con los parámetros proporcionados
        self.connection_string = f'mysql+mysqlconnector://{user}:{password}@{host}:{port}/{db_name}'

    # Método que inserta un DataFrame en una tabla de MySQL
    def insert_dataframe(self, df, table_name):
        try:
            # Crea una conexión a MySQL usando SQLAlchemy
            engine = create_engine(self.connection_string)
            # Inserta el DataFrame en la tabla especificada, reemplazando si existe
            df.to_sql(table_name, con=engine, if_exists='replace', index=False)
            print(f"Datos insertados correctamente en la tabla {table_name} en MySQL.")
        except Exception as e:
            print(f"Error al insertar datos en MySQL: {e}")


# Clase encargada de orquestar el proceso ETL (Extract, Transform, Load)
class ETLProcessor:
    def __init__(self, s3_bucket_path, local_download_path, mysql_params):
        # Inicializa el descargador de S3 y el inserter de MySQL con los parámetros dados
        self.s3_downloader = S3Downloader(s3_bucket_path)
        self.mysql_inserter = MySQLInserter(**mysql_params)
        self.local_download_path = local_download_path  # Ruta local para guardar los archivos descargados

    # Método principal para ejecutar el proceso ETL
    def process(self):
        # Crea el directorio donde se guardarán los archivos si no existe
        os.makedirs(self.local_download_path, exist_ok=True)
        
        # Descarga los archivos desde S3
        files = self.s3_downloader.download_files(self.local_download_path)

        # Si no se descargaron archivos, mostramos un mensaje y terminamos
        if not files:
            print("No se encontraron archivos CSV en el bucket.")
            return

        # Procesamos cada archivo descargado
        for file in files:
            # Construimos la ruta local donde se encuentra el archivo
            local_file_path = os.path.join(self.local_download_path, file.split('/')[-1])
            # Llamamos a la función para procesar el archivo (leerlo y cargarlo a MySQL)
            self._process_file(local_file_path, file)

    # Método privado para procesar un archivo individual
    def _process_file(self, local_file_path, file):
        try:
            # Leemos el archivo CSV usando pandas
            df = pd.read_csv(local_file_path)
            print(f"Archivo CSV {local_file_path} cargado exitosamente.")

            # Construimos el nombre de la tabla para MySQL basado en el nombre del archivo
            table_name = f'table_from_{file.split("/")[-1].split(".")[0]}'
            # Insertamos el DataFrame en MySQL
            self.mysql_inserter.insert_dataframe(df, table_name)

        except Exception as e:
            print(f"Error al procesar el archivo {local_file_path}: {e}")


# Bloque principal que ejecuta el código si se ejecuta directamente
if __name__ == "__main__":
    # Parámetros para la conexión a MySQL
    mysql_params = {
        'host': '54.242.165.173',
        'port': '3306',
        'user': 'root',
        'password': '24112005',
        'db_name': 'prod'
    }
    # Ruta del bucket S3 donde están los archivos
    athena_s3_output = 's3://querys-hoteles/'
    # Ruta local donde se descargarán los archivos
    local_download_path = '/tmp/csv_files'

    # Inicialización del proceso ETL con los parámetros dados
    etl_processor = ETLProcessor(athena_s3_output, local_download_path, mysql_params)
    
    # Ejecutamos el proceso ETL
    etl_processor.process()
