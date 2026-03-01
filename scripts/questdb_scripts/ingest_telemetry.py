import os
import sys
import glob
import pandas as pd
import time
from questdb.ingress import Sender, IngressError, Protocol


if __name__ == "__main__":
    dir = "/Users/aditya/Projects/projects_data/f1db/*/*/race/*/"
    parquet_files = glob.glob(os.path.join(dir, "*.parquet"))
    records = 0

    st = time.time()
    for parquet_file in parquet_files:
        df = pd.read_parquet(parquet_file)
        records += len(df)

        conf = "http::addr=localhost:9000;username=admin;password=quest;"

        try:
            with Sender(Protocol.Http, 'localhost', 9000) as sender:
                sender.dataframe(
                    df,
                    table_name='test_telemetry',
                    symbols=['race', 'driver', 'session'],
                    at='date'
                )
                print(f"Inserted {parquet_file}")
        except IngressError as e:
            sys.stderr.write("Ingesting telemetry data failed.\n")
            sys.stderr.write(str(e))
    end_time = time.time()
    elapsed_time = end_time - st
    print(f"Elapsed time: {elapsed_time} seconds")
    print(f"Records: inserted {records} rows")