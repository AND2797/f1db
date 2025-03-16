import streamlit as st
import os
from src.cache.connection import in_memory_conn
from pathlib import Path
from src.config import config
from src.data_processor.duckdb_utils import attach_to_conn
import glob
import pandas as pd
from fastapi import FastAPI
import pyarrow as pa
import pyarrow.flight as flight


class DuckDBFlightServer(flight.FlightServerBase):
    def __init__(self, host="0.0.0.0", port=8815):
        super().__init__(location=f"grpc://{host}:{port}")
        self.conn = in_memory_conn
        # self.setup_database()

    def do_get(self, context, ticket):
        """Handle incoming queries and return results as Arrow tables."""
        query = ticket.ticket.decode("utf-8")  # Extract SQL query from client request
        try:
            result = self.conn.execute(query).fetch_arrow_table()  # Execute query in DuckDB
            return flight.RecordBatchStream(result)
        except Exception as e:
            raise flight.FlightUnavailableError(f"Query failed: {e}")

    def get_flight_info(self, context, descriptor):
        """Return metadata about a query request."""
        query = descriptor.command.decode("utf-8")
        try:
            result = self.conn.execute(query).fetch_arrow_table()
            schema = result.schema
            return flight.FlightInfo(schema, descriptor,
                                     [flight.FlightEndpoint(ticket=flight.Ticket(query.encode("utf-8")))], -1, -1)
        except Exception as e:
            raise flight.FlightUnavailableError(f"Query failed: {e}")


@st.cache_resource
def spin_arrow():
    in_memory_conn.execute("INSTALL arrow;")
    in_memory_conn.execute("LOAD arrow;")
    in_memory_conn.execute("SET arrow_flight_sql.enable = true;")
    in_memory_conn.execute("SET arrow_flight_sql.uri = 'grpc+tcp://0.0.0.0:8815';")
    in_memory_conn.execute("SET arrow_flight_sql.auth = 'anonymous'")


@st.cache_resource
def serve_flight():
    server = DuckDBFlightServer()
    server.serve()
    print("flight server is running")


def main():
    st.set_page_config(page_title="f1db", layout="wide")
    discover_sources()
    # spin_arrow()
    st.title("f1db")


@st.cache_resource
def discover_sources():
    """
    This function discovers any existing .duckdb databases that may have been created (in the previous session for eg)
    and attaches them to the in-memory connection when the server starts up
    """
    data_root = config.data_root
    globbed_path = os.path.join(data_root, "*", "*", "*", "*.duckdb")
    paths = glob.glob(globbed_path)
    db_info = [[path, Path(path).stem] for path in paths]
    db_info_df = pd.DataFrame(db_info, columns=["path", "attach_as"])
    attach_to_conn(in_memory_conn, db_info_df)


if __name__ == "__main__":
    main()
    serve_flight()
