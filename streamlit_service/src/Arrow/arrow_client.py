import pyarrow.flight as flight
from streamlit_service.src.AppUtils.config_utils import get_property


class ArrowDuckDBClient:

    def __init__(self, conn):
        self.conn = conn
        self._client = flight.connect(conn)

    def execute(self, query):
        ticket = flight.Ticket(query.encode("utf-8"))
        reader = self._client.do_get(ticket)
        df = reader.read_all().to_pandas()
        return df


flight_server = get_property("App", "flight_server")
arrow_duckdb_client = ArrowDuckDBClient(f"{flight_server}")
