import os
import glob
import pandas as pd
from pathlib import Path
from data_service.src.app_utils.config_utils import get_property
from data_service.src.app_utils.logger_utils import setup_logger

logger = setup_logger(get_property("App", "log_file"))

def discover_duckdb_sources():
    data_root = get_property("App", "data_root")
    globbed_path = os.path.join(data_root, "*", "*", "*", "*.duckdb")
    paths = glob.glob(globbed_path)
    db_info = [[path, Path(path).stem] for path in paths]
    logger.info(f"Discovered duckdb sources: {db_info}")
    db_info_df = pd.DataFrame(db_info, columns=["path", "attach_as"])
    return db_info_df