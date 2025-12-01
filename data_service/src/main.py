import duckdb
import pyarrow as pa
import pyarrow.flight as flight
import pandas as pd
import json
import glob
import os
from data_service.src.duck_db.data import discover_duckdb_sources
from data_service.src.duck_db.client import attach_db_to_conn, DuckClient
from data_service.src.app_utils.logger_utils import setup_logger
from data_service.src.app_utils.config_utils import get_property

logger = setup_logger(get_property("App", "log_file"))



class FlightServer(pa.flight.FlightServerBase):

    def __init__(self, location="grpc://0.0.0.0:8815", db_path=":memory:"):
        super().__init__(location=location)
        self._location = location
        self.db_client = DuckClient(db_path)
        self.db_path = db_path
        # self.conn = duckdb.connect(db_path)
        self.data_root = get_property("App", "data_root")
        self.ingest_telemetry_data()
        self.ingest_lap_data()

    def ingest_telemetry_data(self):
        logger.info("Ingesting telemetry data...")
        parquet_files_glob = os.path.join(self.data_root, "*/*/*/telemetry/*.parquet")
        files = glob.glob(parquet_files_glob)
        self.db_client.ctas_parquet(files, "telemetry")

    def ingest_lap_data(self):
        logger.info("Ingesting lap data...")
        parquet_files_glob = os.path.join(self.data_root, "*/*/*/laps.parquet")
        files = glob.glob(parquet_files_glob)
        self.db_client.ctas_parquet(files, "laps")

    def add_data_sources(self):
        data_source_info = discover_duckdb_sources()
        attach_db_to_conn(self.conn, data_source_info)

    def do_get(self, context, ticket):
        sql_query = ticket.ticket.decode('utf-8')
        result = self.db_client.conn.execute(sql_query).fetch_arrow_table()
        return flight.RecordBatchStream(result)

    def do_action(self, context, action):
        """
        If a datasource is newly created, this endpoint is invoked to
        attach the new dataset to the inmemory connection
        :param context:
        :param ticket:
        :param action:
        :return:
        """
        action_type = action.type
        action_body = action.body.to_pybytes().decode()

        if action_type == "attach_new_table":
            data_dict = json.loads(action_body)
            data_source_info = pd.DataFrame(data_dict)
            logger.info(data_source_info)
            attach_db_to_conn(self.conn, data_source_info)


if __name__ == '__main__':
    db_path = os.path.join(get_property("App", "data_root"), "data.db")
    server = FlightServer(db_path=db_path)
    server.serve()