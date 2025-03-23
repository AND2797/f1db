from fastapi import FastAPI
from process_f1_data import F1DataRequest
from arrow_client import arrow_duckdb_client
app = FastAPI()


@app.post("/session/load")
def load_session_data(year: int, race, session):
    data_request = F1DataRequest(year, race, session)
    data_request.write_session_data()
    db_info_json = data_request.get_db_info().to_json()
    arrow_duckdb_client.attach_new_table(db_info_json)
    print("invoked arrow call")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
