import fastf1
from f1_data_service.src.F1Data.fastf1_client import FastF1

def get_session_data(year, race, session):
    session = fastf1.get_session(year, race, session)
    session.load(telemetry=True, laps=True, weather=True)
    return session


def get_laps(year, race, session):
    session = fastf1.get_session(year, race, session)
    session.load(laps=True)
    return session.laps

class F1DataRequest:
    def __init__(self, year, race, session):
        self.year = year
        self.race = race
        self.session = session
        self._f1client = FastF1()
        self.session_data = None

    def load_session(self, **kwargs):
        self.session_data = self._f1client.get_session(self.year, self.race, self.session, **kwargs)



if __name__ == '__main__':
    session_data = get_session_data(2024, 'Spain', 'Q')
    print(session_data)