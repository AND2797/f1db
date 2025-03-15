import duckdb
from src.config import config
import os


def create_duckdb_table(df, table_name, db_path):
    con = duckdb.connect(db_path)
    con.execute(f"CREATE TABLE {table_name} AS SELECT * from df")
    con.close()


