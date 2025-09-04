

class SQLQuery:

    def __init__(self, query, client):
        self.client = client
        self.query = query

    def get_data(self):
        self.client.execute_query(self.query)
