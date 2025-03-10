import streamlit as st
from src.config import config
import glob
import os
from pathlib import Path
import pandas as pd
import re
import duckdb


def get_available_tables():
    #TODO: maybe we need to have one duckdb table per Race.
    # that table should have telemetry data for Q, R, etc.
    data_root = config.data_root
    available_tables = glob.glob(os.path.join(data_root, '*', '*', '*', '*.parquet'))
    relative_paths = [str(Path(table).relative_to(Path(data_root))) for table in available_tables]
    df = pd.DataFrame(relative_paths, index=None, columns=["Available Tables"])
    st.dataframe(df, height=150)


def sql_explorer():
    query = st.text_area("Enter SQL")
    if st.button("Execute"):
        conn = duckdb.connect()
        try:
            if query:
                pattern = r"'([^']+\.parquet)'"

                def replace_with_full_path(match):
                    filename = match.group(1)
                    full_path = os.path.join(config.data_root, filename)
                    return f"'{full_path}'"

                modified_query = re.sub(pattern, replace_with_full_path, query)

                result = conn.execute(modified_query).fetchdf()
                # result['SessionTime'] = pd.to_timedelta(result['SessionTime'])
                # result['Time'] = pd.to_timedelta(result['Time'])
                st.dataframe(result)
        except:
            conn.close()
            print("hehe")
        finally:
            conn.close()



def main():
    st.set_page_config(page_title="SQL Explorer", layout="wide")
    get_available_tables()
    sql_explorer()

if __name__ == '__main__':
    # conn = duckdb.connect()
    # query = "SELECT * FROM '/Users/aditya/Projects/projects_data/f1db/2024/Mexico/Race/telemetry_Race_2024.parquet'"
    # result = conn.execute(query).fetchdf()
    # st.dataframe(result)
    # conn.close()
    main()

