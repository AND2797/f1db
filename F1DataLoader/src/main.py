from fastapi import FastAPI

from F1Data.access_data import F1DataRequest
from F1Data.process_data import F1DataProcessor
from F1DataLoader.src.Parquet.parquet_facade import ParquetFacade
from AppUtils.logger_utils import setup_logger
from AppUtils.config_utils import get_property


app = FastAPI()

logger = setup_logger(get_property("App", "log_file"))


@app.post("/session/load")
def load_session_data(year: int, race, session):
    logger.info(f"Received load session data request: {year}, {race}, {session}")

    request = F1DataRequest(year, race.lower(), session.lower())

    optional = {'telemetry': True,
                'laps': True,
                'weather': True}
    request.load_session(**optional)

    processor = F1DataProcessor(request, ParquetFacade(), dict()) #TODO: pass DuckDB
    processor.write_session_data()

    # processor.attach_tables()
    # data_request.write_session_data()
    # db_info_json = data_request.get_db_info().to_json()
    # arrow_duckdb_client.attach_new_table(db_info_json)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
