# funciones de soporte

import inspect
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
