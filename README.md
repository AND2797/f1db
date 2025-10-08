# f1db: Historical F1 Data platform

The aim of this project is to setup a persistent historical f1 database with capability to explore and manipulate data in native SQL.


The project has 3 pieces
- f1_data_service - A REST service that fetches data from FastF1 and dumps it in parquet files. It creates persistent .duckdb databases from the parquet files
- data_service - An ArrowFlightServer that links the .duckdb databases with the UI.
- streamlit_Service - A UI which provides some rudimentary analytics and allows exploring various datasets using SQL

The motivation is to allow users to store and collect data over time and perform analytics using SQL instead of relying on the python api.

