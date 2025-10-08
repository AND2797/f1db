import duckdb
import pandas as pd

class DuckDBClient:
    def __init__(self, conn_str):
        self.conn = duckdb.connect(conn_str)

    def attach_db(self, db_path, attach_as):
        """
        :param db_path: absolute path of .db file
        :param attach_as: the table name to attach to current connection as
        :return:
        """
        query = f"ATTACH '{db_path}' as {attach_as};"
        self.conn.execute(query)

    def create_table(self, df, table_name, path):
        """
        :param df: dataframe to be written to .db
        :param table_name: name of the duckdb table to be created
        :param path: absolute path of .db file
        :return:
        """
        con = duckdb.connect(path)
        con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * from df")
        con.close()

    def discover_sources(self, path):
        pass



def attach_db_to_conn(conn, database_info: pd.DataFrame):
    query = "ATTACH '{0}' as {1} (READ_ONLY);"
    database_info.apply(lambda db_info: conn.execute(query.format(db_info['path'], db_info['attach_as'])), axis=1)