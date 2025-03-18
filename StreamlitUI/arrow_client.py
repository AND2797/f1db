import pyarrow.flight as flight


class ArrowDuckDBClient:

    def __init__(self, conn):
        self.conn = conn
        self._client = flight.connect(conn)

    def execute(self, query):
        ticket = flight.Ticket(query.encode("utf-8"))
        reader = self._client.do_get(ticket)
        df = reader.read_all().to_pandas()
        return df


arrow_duckdb_client = ArrowDuckDBClient("grpc://localhost:8815")
