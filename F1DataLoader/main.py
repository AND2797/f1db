from fastapi import FastAPI
from process_f1_data import get_session_data, write_laps_data_for_session, write_telemetry_for_session, write_session_data
from arrow_client import arrow_duckdb_client
app = FastAPI()


@app.post("/session/load")
def load_session_data(year: int, race, session):
    db_info = write_session_data(year, race, session)
    print(f"written session date to {db_info}")
    db_info_json = db_info.to_json()
    arrow_duckdb_client.attach_new_table(db_info_json)
    print("invoked arrow call")

# TODO:
# need to link with arrow flight service to attach .duckdb when it's ready
# need to interface with streamlit UI to load data


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
