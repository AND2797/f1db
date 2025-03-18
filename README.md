# f1db: Historical F1 Data platform

What?
This project will let you build your own persistent F1 database. 

Why?
- This setup lets you create data sets for any F1 race (provided FastF1 can get it for you) without having to re-write FastF1 API code.
- A better experience exploring data with the help of native SQL (backed by DuckDB)
- Easy to view data across multiple sessions, years with the help of SQL joins 

How?
The project has 3 pieces - 
- F1DataLoader - Fetches data from FastF1 and dump it in parquet files and create persistent .duckdb databases
- ArrowFlightService - Spins up an ArrowFlightServer that exposes .duckdb databases created by F1DataLoader
- Streamlit UI - Just a UI for running SQL queries and viewing available datasets


