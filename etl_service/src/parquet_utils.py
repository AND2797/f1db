import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from pathlib import Path


def write_parquet(df, path: Path):
    table = pa.Table.from_pandas(df)
    pq.write_table(table, str(path))


def read_parquet(path):
    df = pd.read_parquet(path, engine='pyarrow')
    return df




if __name__ == '__main__':
    df = read_parquet("test.parquet")
    print(df)
    # data = get_session_data(2024, 'Monaco', 'Q')
    # import os
    # path = os.getcwd()
    # write_parquet(data.laps, f"{path}/test.parquet")
