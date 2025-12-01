import duckdb
import pandas as pd
from data_service.src.app_utils.logger_utils import setup_logger
from data_service.src.app_utils.config_utils import get_property

logger = setup_logger(get_property("App", "log_file"))

class DuckClient:
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

    def ctas_df(self, df, table_name):
        """
        :param df: dataframe to be written to .db
        :param table_name: name of the duckdb table to be created
        :param path: absolute path of .db file
        :return:
        """
        query = f"CREATE OR REPLACE TABLE {table_name} AS SELECT * from df"
        self.conn.execute(query)

    def ctas_parquet(self, files, table_name):
        """
        :param files: parquet files used for table creation
        :param table_name: name of the duckdb table to be created
        :param path: absolute path of .db file
        :return:
        """
        query = f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_parquet({files});"
        self.conn.execute(query)

    def query(self, sql):
        return self.conn.execute(sql)

    def discover_sources(self, path):
        pass

    def __aexit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()



def attach_db_to_conn(conn, database_info: pd.DataFrame):
    query = "ATTACH '{0}' as {1} (READ_ONLY);"
    database_info.apply(lambda db_info: conn.execute(query.format(db_info['path'], db_info['attach_as'])), axis=1)