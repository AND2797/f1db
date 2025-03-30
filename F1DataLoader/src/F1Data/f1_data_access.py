import fastf1


def get_session_data(year, race, session):
    session = fastf1.get_session(year, race, session)
    session.load(telemetry=True, laps=True, weather=True)
    return session


def get_laps(year, race, session):
    session = fastf1.get_session(year, race, session)
    session.load(laps=True)
    return session.laps


if __name__ == '__main__':
    session_data = get_session_data(2024, 'Spain', 'Q')
    print(session_data)