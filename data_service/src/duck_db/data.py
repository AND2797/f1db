import os
import glob
import pandas as pd
from pathlib import Path
from data_service.src.app_utils.config_utils import get_property


def discover_duckdb_sources():
    data_root = get_property("App", "data_root")
    globbed_path = os.path.join(data_root, "*", "*", "*", "*.duckdb")
    paths = glob.glob(globbed_path)
    db_info = [[path, Path(path).stem] for path in paths]
    db_info_df = pd.DataFrame(db_info, columns=["path", "attach_as"])
    return db_info_df
