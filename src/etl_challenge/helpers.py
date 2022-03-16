# funciones de soporte

import os
import pandas as pd
from datetime import datetime

def print_log(string: str):
    # imprimir el log de trabajo en consola
    now = datetime.now()
    print(f"{now.strftime('%d/%m/%Y %H:%M:%S')} | {string}")
    return None

def add_id(dataframe: pd.DataFrame, col_name: str):
    # agregar una columna de id a una tabla
    ids = pd.Series(range(1, len(dataframe.index) + 1))
    return_df = dataframe.copy()
    return_df[col_name] = ids
    return return_df

def clean_merged(dataframe: pd.DataFrame, col_name: str, deletes: list):
    # limpiar los dataframes recien unidos
    return_df = dataframe.rename({"value": col_name}, axis=1, copy=True)
    return_df = return_df.drop(deletes, axis=1)
    return return_df

def export_data(dataframe: pd.DataFrame, filename: str):
    # exportar dataframes a csv en data/results
    print_log(string=f"Exportando objeto {filename} a disco...")
    data_folder = os.path.join(os.getcwd(), "data", "results")
    new_file = os.path.join(data_folder, filename)
    if not os.path.exists(data_folder):
        print_log(string="Creando carpeta de resultados...")
        os.makedirs(data_folder)
        print_log(string="Directorio de resultados creado.")
    if os.path.exists(new_file):
        print_log(string="Archivo de salida ya existe. Reemplazando...")
        os.remove(new_file)
    else:
        print_log(string="Escribiendo archivo de salida...")
    try:
        dataframe.to_csv(new_file, sep=",", index=False, header=True)
    except:
        print_log(string="Error al exportar los datos. Por favor, compruebe y reintente.")
        raise Exception
    finally:
        print_log(string="Proceso de exportado de datos finalizado.")
