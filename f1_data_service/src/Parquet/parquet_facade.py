import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from pathlib import Path


class BaseParquetFacade:

    @staticmethod
    def write(df: pd.DataFrame, path: Path):
        raise NotImplementedError()

    @staticmethod
    def read(path: Path):
        raise NotImplementedError()

class ParquetFacade(BaseParquetFacade):

    def __init__(self):
        super().__init__()

    @staticmethod
    def write(df: pd.DataFrame, path: Path):
        table = pa.Table.from_pandas(df)
        pq.write_table(table, str(path))

    @staticmethod
    def read(path: Path):
        df = pd.read_parquet(path, engine='pyarrow')
        return df
