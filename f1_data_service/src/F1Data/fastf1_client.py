import fastf1


class FastF1:

    def __init__(self):
        pass

    @staticmethod
    def validate(race, session, schedule):
        race = race.replace(' ', '_').lower()
        schedule['FormattedEventName'] = schedule['EventName'].str.replace(' ','_').str.lower()
        assert race in schedule['FormattedEventName'].unique(), "Not a valid race"

        schedule = schedule[schedule['FormattedEventName'] == race]
        session = session.replace(' ', '_').lower()
        session_columns = ['Session1', 'Session2', 'Session3', 'Session4', 'Session5']
        possible_sessions = set()
        for s in session_columns:
            possible_sessions.update(set(schedule[s].str.replace(' ', '_').str.lower().tolist()))


        assert session in possible_sessions, "Not a valid session"

    @staticmethod
    def get_session(year, race, session, **kwargs):
        schedule = fastf1.get_event_schedule(year)
        FastF1.validate(race, session, schedule)

        session = fastf1.get_session(year, race, session)

        telemetry = kwargs.get('telemetry', False)
        laps = kwargs.get('laps', False)
        weather = kwargs.get('weather', False)

        optional_loads = telemetry or laps or weather

        if optional_loads:
            session.load(telemetry=telemetry, laps=laps, weather=weather)

        return session