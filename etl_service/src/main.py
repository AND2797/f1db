from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional, List
import asyncio

from f1_data.access_data import F1DataRequest
from f1_data.process_data import F1DataProcessor
from etl_service.src.parquet.parquet_facade import ParquetFacade
from app_utils.logger_utils import setup_logger
from app_utils.config_utils import get_property


app = FastAPI()

logger = setup_logger(get_property("App", "log_file"))

class SessionLoadRequest(BaseModel):
    year: int
    race: str
    session: str


@app.post("/session/load")
async def load_session_data(session_load_requests: List[SessionLoadRequest]):
    tasks = [
        asyncio.to_thread(process_request, req) for req in session_load_requests
    ]
    await asyncio.gather(*tasks)
    return {"message": f"Successfully processed {len(session_load_requests)} session load requests."}

def process_request(request: SessionLoadRequest):
    data_request = load_data(request)
    process_data(data_request)

def load_data(request: SessionLoadRequest) -> F1DataRequest:
    year = request.year
    race = request.race
    session = request.session
    f1_data_request = F1DataRequest(year, race, session)
    optional = {'telemetry': True, 'laps': True, 'weather': True}
    f1_data_request.load_session(**optional)
    return f1_data_request

def process_data(f1_data_request: F1DataRequest) -> None:
    f1_data_processor = F1DataProcessor(f1_data_request, ParquetFacade(), dict())
    f1_data_processor.write_session_data()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
