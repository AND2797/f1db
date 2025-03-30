import requests
import asyncio


class F1DataLoaderClient:
    def __init__(self, base_url):
        self.base_url = base_url

    async def load_session_data_async(self, year: int, race: str, session: str):
        return await asyncio.to_thread(self.load_session_data, year, race, session)

    def load_session_data(self, year: int, race: str, session: str):
        url = f"{self.base_url}/session/load"
        params = {"year": year, "race": race, "session": session}
        response = requests.post(url, params=params)

        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()




