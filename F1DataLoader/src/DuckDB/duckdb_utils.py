import duckdb
import os
import glob
import pandas as pd

# Maybe rename this to client. Create duckdb client

def create_duckdb_table(df, table_name, db_path):
    con = duckdb.connect(db_path)
    con.execute(f"CREATE TABLE {table_name} AS SELECT * from df")
    con.close()


def attach_to_conn(conn, database_info: pd.DataFrame):
    query = "ATTACH '{0}' as {1};"
    database_info.apply(lambda db_info: conn.execute(query.format(db_info['path'], db_info['attach_as'])), axis=1)






