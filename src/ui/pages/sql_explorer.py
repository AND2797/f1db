import streamlit as st
from src.config import config
import os
import re
from src.cache.connection import in_memory_conn


def get_available_tables():
    """
    This function displays the available databases and tables on the in-memory duckdb instance
    :return:
    """
    #TODO: maybe we need to have one duckdb table per Race.
    # that table should have telemetry data for Q, R, etc.
    # data_root = config.data_root
    query = "SELECT * FROM information_schema.tables;"
    df = in_memory_conn.execute(query).fetchdf()
    df = df.rename({'table_catalog': 'database', 'table_name': 'table'}, axis=1)
    df = df[['database', 'table']]
    df = df.groupby("database")["table"].apply(list).reset_index().set_index(df.columns[0])
    st.dataframe(df, height=150)


def sql_explorer():
    query = st.text_area("Enter SQL")
    if st.button("Execute"):
        conn = in_memory_conn
        try:
            def replace_with_full_path(match):
                filename = match.group(1)
                full_path = os.path.join(config.data_root, filename)
                return f"'{full_path}'"

            if query:
                pattern = r"'([^']+\.parquet)'"


                modified_query = re.sub(pattern, replace_with_full_path, query)

                result = conn.execute(modified_query).fetchdf()
                # st.dataframe is not able to render
                df_html = result.to_html(index=False, escape=False)
                st.markdown(html_code + df_html, unsafe_allow_html=True)
        except Exception as e:
            print(e)
            print("hehe")
        finally:
            print("finally hehe")

# TODO: clean this up.
# Custom CSS for word-wrapping with fixed width
html_code = """
<style>
    table {
        width: 100%;
        border-collapse: collapse;
        font-family: Arial, sans-serif;
    }
    th, td {
        padding: 10px;
        border: 1px solid #ddd;
        text-align: left;
        vertical-align: top;  /* Ensures text stays at the top */
        white-space: normal;  /* Allows text wrapping */
        word-wrap: break-word; /* Forces text to break if too long */
        max-width: 200px;  /* Restricts width to prevent stretching */
        overflow-wrap: break-word; /* Alternative wrapping for extra support */
    }
    th {
        background-color: #4CAF50;
        color: white;
    }
    tr:hover {
        background-color: #f5f5f5;
    }
</style>
"""


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

