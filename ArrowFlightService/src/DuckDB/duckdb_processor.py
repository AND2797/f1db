import pandas as pd


def attach_db_to_conn(conn, database_info: pd.DataFrame):
    query = "ATTACH '{0}' as {1};"
    database_info.apply(lambda db_info: conn.execute(query.format(db_info['path'], db_info['attach_as'])), axis=1)