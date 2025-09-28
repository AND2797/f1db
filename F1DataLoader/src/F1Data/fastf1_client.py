import fastf1


class FastF1:

    def __init__(self):
        pass

    @staticmethod
    def get_session(year, race, session, **kwargs):
        session = fastf1.get_session(year, race, session)

        telemetry = kwargs.get('telemetry', False)
        laps = kwargs.get('laps', False)
        weather = kwargs.get('weather', False)

        optional_loads = telemetry or laps or weather

        if optional_loads:
            session.load(telemetry=telemetry, laps=laps, weather=weather)

        return session