import fastf1
from fastapi import FastAPI
from process_f1_data import get_session_data, write_laps_data_for_session, write_telemetry_for_session

app = FastAPI()


@app.post("/session/load")
def load_session_data(year: int, race, session):
    session_data = get_session_data(year, race, session)
    write_laps_data_for_session(year, race, session, session_data)
    write_telemetry_for_session(year, race, session, session_data)

# TODO:
# need to link with arrow flight service to attach .duckdb when it's ready
# need to interface with streamlit UI to load data


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
