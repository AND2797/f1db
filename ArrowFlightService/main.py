import duckdb
import pyarrow as pa
import pyarrow.flight as flight
from pathlib import Path
from config import data_root
from data import discover_duckdb_sources
from duckdb_processor import attach_db_to_conn


class FlightServer(pa.flight.FlightServerBase):

    def __init__(self, location="grpc://0.0.0.0:8815", db_path=":memory:"):
        super().__init__(location=location)
        self._location = location
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        self.add_data_sources()

    def add_data_sources(self):
        data_source_info = discover_duckdb_sources()
        attach_db_to_conn(self.conn, data_source_info)

    def do_get(self, context, ticket):
        sql_query = ticket.ticket.decode('utf-8')
        result = self.conn.execute(sql_query).fetch_arrow_table()
        return flight.RecordBatchStream(result)



if __name__ == '__main__':
    server = FlightServer()
    server.serve()