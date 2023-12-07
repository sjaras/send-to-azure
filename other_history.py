import pandas as pd
import pyodbc
import logging


# Configuración del registro
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

server = 'mined.database.windows.net'
database = 'mined-ventas'
username = 'adminMined'
password = 'passwordMined1.'
driver = '{ODBC Driver 17 for SQL Server}'

connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'


# Establece la conexión
conn = pyodbc.connect(connection_string)

table_name = 'Ventas'

# Crea un cursor
cursor = conn.cursor()

truncate_query = f'TRUNCATE TABLE {table_name}'
print("Vaciando la tabla ventas...")

# cursor.execute(truncate_query)

print("Tabla de Ventas limpia✔")


conn.commit()

# Ruta al archivo CSV
csv_file_path = 'salesfixed2.csv'


# Tamaño del lote (ajusta según tus necesidades)
batch_size = 20000

cursor = conn.cursor()

# Desactiva la confirmación automática
conn.autocommit = False

# insert_query = f'INSERT INTO {table_name} VALUES ({", ".join(["?"] * len(pd.read_csv(csv_file_path, nrows=1).columns))})'

start_row = 1000  # Cambia esto al número de fila desde el cual deseas comenzar a leer

df = pd.read_csv(csv_file_path, skiprows=range(1, start_row))

# df = pd.read_csv(csv_file_path)


row_count = 0
# Lee y carga datos por lotes
# for chunk in pd.read_csv(csv_file_path, chunksize=batch_size):
#     # Convierte el DataFrame de pandas a una lista de tuplas
#     rows = [tuple(row) for row in chunk.itertuples(index=False, name=None)]

#     # Construye la consulta de inserción con parámetros
#     # insert_query = f'INSERT INTO {table_name} VALUES ({", ".join(["?"] * len(chunk.columns))})'

#     # Ejecuta la consulta de inserción
#     cursor.executemany(insert_query, rows)

#     # Confirma los cambios en la base de datos
#     conn.commit()

#     # Actualiza el contador y muestra el progreso
#     row_count += len(chunk)
#     print(f'Procesados {row_count} registros hasta ahora...')
#     # print(f'Procesados {len(rows)} registros hasta ahora...')

# conn.autocommit = True

# # Cierra el cursor y la conexión
# cursor.close()
# conn.close()
print("Preparando la inserción de datos...")
for start in range(0, len(df), batch_size):
    batch = df.iloc[start:start + batch_size]

    # Convierte el DataFrame de pandas a una lista de tuplas
    rows = [tuple(row) for row in batch.itertuples(index=False, name=None)]

    # Prepara la consulta de inserción
    insert_query = f'INSERT INTO {table_name} VALUES ({", ".join(["?"] * len(df.columns))})'

    # Ejecuta la consulta de inserción
    cursor.executemany(insert_query, rows)
    row_count += len(start)
    # print(f'Procesados {row_count} registros hasta ahora...')
    print(f'Procesados {row_count} registros hasta ahora...')
    # Confirma los cambios en la base de datos
    conn.commit()

# Activa la confirmación automática al final
conn.autocommit = True

# Cierra el cursor y la conexión
cursor.close()
conn.close()
