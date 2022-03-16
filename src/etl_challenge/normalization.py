# definición de funciones de adquisición y normalización de datos

from etl_challenge import helpers as hp
import sys
import pandas as pd
import numpy as np


def read_csv(file_to_read: str, check_output: bool=False):
    # leer los csv fuente
    hp.print_log(string=f"Leyendo datos desde archivo {file_to_read}...")
    try:
        data = pd.read_csv(file_to_read, sep=",")
        if check_output:
            hp.print_log(string="A continuación se muestran las primeras 5 filas del archivo leído:")
            print(data.head(n=5))
        return data
    except:
        hp.print_log(string=f"Error en la lectura de {file_to_read}. Revise la ruta y el archivo.")
        raise Exception
    finally:
        hp.print_log(string="Operación de lectura de datos terminada.")
    return None

def normalize_values(data_pd: pd.DataFrame, column: str, check_output: bool=False):
    # normalizar tablas con ids y sin duplicados
    hp.print_log(string=f"Deduplicando y normalizando el objeto {column}...")
    try:
        col_list = data_pd[column]
        data_col = col_list.unique()
        data_ids = pd.Series(range(1, len(data_col) + 1))
        data_return = pd.DataFrame(list(zip(data_ids, data_col)), columns=["id", "value"])
        if check_output:
            hp.print_log(string="A continuación se muestra la data normalizada:")
            print(data_return)
        return data_return
    except:
        hp.print_log(string="Error en la normalización de la data. Revise los nombres de columnas.")
        raise Exception
    finally:
        hp.print_log(string="Operación de limpieza de datos terminada.")
    return None

def clean_values(source_pd: pd.DataFrame, normal_pd: pd.DataFrame, on_x:str, on_y: str="value", check_output: bool=False):
    # limpiar las tablas de los valores de texto y dejar los ids
    hp.print_log(string="Limpiando datos y reemplazando valores por ids...")
    try:
        merge_pd = pd.merge(left=source_pd, right=normal_pd, how="left", left_on=on_x, right_on=on_y, copy=True)
        new_col = on_x + "_id"
        rename_pd = merge_pd.rename({"id": new_col}, axis=1, copy=True)
        return_pd = rename_pd.drop(labels=["value", on_x], axis=1)
        if check_output:
            hp.print_log(string="A continuación se muestra la data limpia:")
            print(return_pd)
        return return_pd
    except:
        hp.print_log(string="Error en la limpieza de la data. Revise los nombres de columnas.")
        raise Exception
    finally:
        hp.print_log(string="Operación de limpieza de datos terminada.")
    return None

def enrich_data(original_pd: pd.DataFrame, extra_pd: pd.DataFrame, on_x: str, on_y: str, check_output: bool=False):
    # enriquecer la data basada en ids
    hp.print_log(string="Enriqueciendo datos...")
    try:
        merge_pd = pd.merge(left=original_pd, right=extra_pd, how="left", left_on=on_x, right_on=on_y, copy=True)
        return_pd = merge_pd.drop(labels=["id"], axis=1)
        if check_output:
            hp.print_log(string="A continuación se muestra la data enriquecida:")
            print(return_pd)
        return return_pd
    except:
        hp.print_log(string="Error en el enriquecimiento de datos. Revise los nombres de columnas.")
        raise Exception
    finally:
        hp.print_log(string="Operación de enriquecimiento de datos terminada.")
    return None

def get_full_score_data(reviews: pd.DataFrame, consoles: pd.DataFrame, check_output:bool = False):
    # obtener una vista con toda la data
    try:
        merge_pd = pd.merge(left=reviews, right=consoles, left_on="console_id", right_on="id", how="left", copy=True)
        merge_pd = merge_pd.drop(["id", "value"], axis=1)
        if check_output:
            hp.print_log(string="Se muestran los 10 primeros registros de la vista:")
            print(merge_pd.head(10))
        return merge_pd
    except:
        hp.print_log(string="Error generando la vista de datos.")
        raise Exception
    finally:
        hp.print_log(string="Operación de creación de vista terminada.")
    return None
