# f1db: Historical F1 Data platform

The aim of this project is to setup a persistent historical f1 database for race telemetry data and explore data
using SQL. 

The project has 3 pieces
- F1DataLoader - A REST service that fetches data from FastF1 and dumps it in parquet files. It creates persistent .duckdb databases from the parquet files
- ArrowFlightService - An ArrowFlightServer that links the .duckdb databases with the UI.
- Streamlit UI - A UI which provides some rudimentary analytics and allows exploring various datasets using SQL


The motivation of this project was to have something like a queryable database of F1 telemetry for custom analysis.

