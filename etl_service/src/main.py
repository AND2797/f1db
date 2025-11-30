from fastapi import FastAPI

from f1_data.access_data import F1DataRequest
from f1_data.process_data import F1DataProcessor
from etl_service.src.parquet.parquet_facade import ParquetFacade
from app_utils.logger_utils import setup_logger
from app_utils.config_utils import get_property


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

    processor = F1DataProcessor(request, ParquetFacade(), dict()) #TODO: pass duck_db
    processor.write_session_data()

    # processor.attach_tables()
    # data_request.write_session_data()
    # db_info_json = data_request.get_db_info().to_json()
    # arrow_duckdb_client.attach_new_table(db_info_json)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
