import io

import streamlit as st
from streamlit_service.src.Arrow.ArrowClient import arrow_duckdb_client


def get_available_tables():
    """
    This function displays the available databases and tables on the in-memory duckdb instance
    :return:
    """
    query = "SELECT * FROM information_schema.tables;"
    df = arrow_duckdb_client.execute(query)
    df = df.rename({'table_catalog': 'database', 'table_name': 'table'}, axis=1)
    df = df[['database', 'table']]
    df = df.groupby("database")["table"].apply(list).reset_index().set_index(df.columns[0])
    st.dataframe(df, height=150)


def sql_explorer():
    query = st.text_area("Enter SQL")
    # query = st_ace(language="sql", height=200)
    if st.button("Execute"):
        try:
            df = arrow_duckdb_client.execute(query)
            st.dataframe(df, height=150)

            if not df.empty:
                st.download_button(label="Download .parquet",
                                   data=write_parquet_to_buffer(df),
                                   file_name="data.parquet",
                                   mime="application/octet-stream")

        except Exception as e:
            print(e)


def write_parquet_to_buffer(df):
    buffer = io.BytesIO()
    df.to_parquet(buffer, engine='pyarrow')
    buffer.seek(0)
    return buffer


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

