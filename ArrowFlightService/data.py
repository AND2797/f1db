import os
import glob
import pandas as pd
from pathlib import Path
from config import data_root


def discover_duckdb_sources():
    globbed_path = os.path.join(data_root, "*", "*", "*", "*.duckdb")
    paths = glob.glob(globbed_path)
    db_info = [[path, Path(path).stem] for path in paths]
    db_info_df = pd.DataFrame(db_info, columns=["path", "attach_as"])
    return db_info_df
    attach_to_conn(in_memory_conn, db_info_df)
