# script principal del proceso etl

# imports
import os
import sys
import pandas as pd
from pandasql import sqldf
from etl_challenge import helpers as hp
from etl_challenge import normalization as nm

# definiciones globales
files_to_read = {
    "consolas": os.path.join(os.getcwd(), "data", "consoles.csv"), 
    "puntajes": os.path.join(os.getcwd(), "data", "result.csv")
}

reviews_pd = ["review_id", "name_id", "console_id", "userscore", "date"]
consola_pd = ["id", "company_id", "value"]

pysqldf = lambda q: sqldf(q, globals())

# funciones
def answers(name: pd.DataFrame, console: pd.DataFrame, company: pd.DataFrame, sort: str, type: int):
    # primera respuesta: diez mejores/peores por consola/compañia
    if type == 1:
        query = f"""
        SELECT
            name_id
            ,console_id
            ,company_id
            ,AVG(userscore) AS userscore
        FROM full_data
        GROUP BY
            name_id
            ,console_id
            ,company_id
        ORDER BY userscore {sort}
        LIMIT 10
        """
    elif type == 2:
        query = f"""
        SELECT
            name_id
            ,AVG(userscore) AS userscore
        FROM full_data
        GROUP BY
            name_id
        ORDER BY userscore {sort}
        LIMIT 10
        """
    answer_df = pysqldf(query)
    answer_df = pd.merge(left=answer_df, right=name, left_on="name_id", right_on="id", how="left", copy=True)
    answer_df = hp.clean_merged(dataframe=answer_df, col_name="name", deletes=["name_id", "id"])
    if type == 1:
        answer_df = pd.merge(left=answer_df, right=console[["id", "value"]], left_on="console_id", right_on="id", how="left", copy=True)
        answer_df = hp.clean_merged(dataframe=answer_df, col_name="console", deletes=["console_id", "id"])
        answer_df = pd.merge(left=answer_df, right=company, left_on="company_id", right_on="id", how="left", copy=True)
        answer_df = hp.clean_merged(dataframe=answer_df, col_name="company", deletes=["company_id", "id"])
    return answer_df

if __name__ == "__main__":
    # obtener los archivos fuente
    for file in files_to_read:
        try:
            locals()[file] = nm.read_csv(file_to_read=files_to_read[file], check_output=False)
        except:
            hp.print_log(string=f"No se pudo leer el archivo {files_to_read[file]}.\nSaliendo ahora...")
            sys.exit(1)

    # normalizar valores
    company_tbl = nm.normalize_values(data_pd=consolas, column="company", check_output=False)
    consolas_dim = nm.normalize_values(data_pd=consolas, column="console", check_output=False)
    title_tbl = nm.normalize_values(data_pd=puntajes, column="name", check_output=False)

    # limpiar valores
    console_pd = nm.clean_values(source_pd=consolas, normal_pd=company_tbl, on_x="company", check_output=False)
    consolas_pd = nm.clean_values(source_pd=console_pd, normal_pd=consolas_dim, on_x="console", check_output=False)
    titles_pd = nm.clean_values(source_pd=puntajes, normal_pd=title_tbl, on_x="name", check_output=False)
    titulos_pd = nm.clean_values(source_pd=titles_pd, normal_pd=consolas_dim, on_x="console", check_output=False)
    # agregar el id de reviews
    titulos_rnm = hp.add_id(dataframe = titulos_pd, col_name="review_id")

    # obtener el dataframe definitivo
    scores_tbl = titulos_rnm[reviews_pd]

    # enriquecer la data de consolas
    consoles_tbl = nm.enrich_data(original_pd=consolas_pd, extra_pd=consolas_dim, on_x="console_id", on_y="id", check_output=False)
    # limpiar la tabla de consolas
    consoles_tbl = consoles_tbl.rename({"console_id": "id"}, axis=1, copy=True)
    consoles_tbl = consoles_tbl[consola_pd]

    # limpiar el espacio de trabajo temporal
    del titulos_pd, titulos_rnm, titles_pd, consolas_pd, console_pd, consolas_dim, consolas, puntajes

    # print(company_tbl)
    # print(title_tbl)
    # print(scores_tbl)
    # print(consoles_tbl)

    # obtener vista con toda la data para hacer queries
    full_data = nm.get_full_score_data(reviews=scores_tbl, consoles=consoles_tbl)

    # obtener respuestas
    top_company_console = answers(name=title_tbl, console=consoles_tbl, company=company_tbl, sort="DESC", type=1)
    worst_company_console = answers(name=title_tbl, console=consoles_tbl, company=company_tbl, sort="ASC", type=1)
    top_all = answers(name=title_tbl, console=consoles_tbl, company=company_tbl, sort="DESC", type=2)
    worst_all = answers(name=title_tbl, console=consoles_tbl, company=company_tbl, sort="ASC", type=2)

    # print(top_company_console)
    # print(worst_company_console)
    # print(top_all)
    # print(worst_all)

    # exportar respuestas
