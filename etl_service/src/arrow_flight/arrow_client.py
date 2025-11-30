import pyarrow.flight as flight


class ArrowDuckDBClient:

    def __init__(self, conn):
        self.conn = conn
        self._client = flight.connect(conn)

    def attach_new_table(self, json):
        """
        When a new .duckb table is created via dataloader, this call will get invoked to update the arrow_flight source
        :param json:
        :return:
        """
        action = flight.Action("attach_new_table", json.encode())
        results = self._client.do_action(action)
        return results


arrow_duckdb_client = ArrowDuckDBClient("grpc://localhost:8815")
