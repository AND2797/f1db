import pandas as pd
from F1DataLoader.src.AppUtils.config_utils import get_property
from F1DataLoader.src.AppUtils.logger_utils import setup_logger
from F1DataLoader.src.parquet_utils import write_parquet
from F1DataLoader.src.DuckDB.duckdb_utils import create_duckdb_table
from F1DataLoader.src.F1Data.f1_data_access import get_session_data
import os
from pathlib import Path

logger = setup_logger(get_property("App", "log_file"))


class F1DataRequest:
    def __init__(self, year, race, session):
        self.year = year
        self.race = race
        self.session = session
        self.duck_table = f"{race.lower()}_{session.lower()}_{year}.duckdb"
        # TODO: move property calls to their own utils file
        self.data_root = Path(get_property("App", "data_root"))
        self.data_dir = self.data_root.joinpath(f"{year}", f"{race.lower()}", f"{session.lower()}")
        self.raw_data_dir = self.data_dir.joinpath("raw")
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.raw_data_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.data_dir.joinpath(self.duck_table)
        self.db_info = pd.DataFrame(columns=['path', 'attach_as'], data=[[str(self.db_path),
                                                                          f"{race.lower()}_{session.lower()}_{year}"]])
        self.session_data = get_session_data(year, race, session)

    def get_db_info(self):
        return self.db_info

    def write_laps_data(self):
        file_name = f"laps_{self.session.lower()}_{self.year}.parquet"
        session_info = self.session_data.session_info
        year = session_info["StartDate"].year
        race = session_info["Meeting"]["Circuit"]["ShortName"]
        session = session_info["Type"]

        laps_df = self.session_data.laps.copy()
        laps_df['year'] = year
        # race != country
        laps_df['race'] = race
        laps_df['session'] = session
        laps_df.columns = (laps_df.columns
                             .str.replace('(?<=[a-z])(?=[A-Z])', '_', regex=True)
                             .str.lower())
        laps_df = timedelta_to_seconds(laps_df)
        file_path = self.raw_data_dir.joinpath(file_name)
        write_parquet(laps_df, file_path)
        create_duckdb_table(laps_df, "laps", str(self.db_path))
        logger.info(f"Completed writing laps data")

    def write_telemetry_data(self):
        telemetry_df = process_telemetry_data(self.year, self.race, self.session,
                                              self.session_data)

        file_name = f"telemetry_{self.session.lower()}_{self.year}.parquet"
        file_path = self.raw_data_dir.joinpath(file_name)
        write_parquet(telemetry_df, file_path)
        create_duckdb_table(telemetry_df, "telemetry", str(self.db_path))
        logger.info(f"Completed writing telemetry data")

    def write_session_data(self):
        self.write_laps_data()
        self.write_telemetry_data()


def process_telemetry_data(year, race, session, session_data):
    drivers = session_data.drivers
    all_telemetry = []
    for driver in drivers:
        laps = session_data.laps.pick_drivers(driver)
        telemetry = get_all_telemetry(laps)
        # streamlit is unable to render timedelta[ns]. converting it to seconds
        telemetry['year'] = year
        telemetry['race'] = race.lower()
        telemetry['session'] = session.lower()
        telemetry.columns = (telemetry.columns
                             .str.replace('(?<=[a-z])(?=[A-Z])', '_', regex=True)
                             .str.lower())
        telemetry = timedelta_to_seconds(telemetry)
        all_telemetry.append(telemetry)

    all_telemetry_df = pd.concat(all_telemetry)
    return all_telemetry_df


def get_all_telemetry(laps):
    #TODO: find a better way to do this, basically we just need to add the LapNumber to telemetry
    telemetry_list = []
    for index, each_lap in laps.iterrows():
        try:
            telemetry = each_lap.get_telemetry()
            #TODO: check if data is getting added correctly?
            telemetry["Driver"] = each_lap["Driver"]
            telemetry["DriverNumber"] = each_lap["DriverNumber"]
            telemetry["LapNumber"] = each_lap["LapNumber"]
            telemetry_list.append(telemetry)
        except Exception as e:
            print(f"{e}")
            print(f"Exception in getting telemetry for driver: {each_lap['Driver']}")
            continue

    telemetry_df = pd.concat(telemetry_list)
    return telemetry_df


def get_data_path(year, race, session):
    if session.lower() == "q" or session.lower() == "qualifying" or session.lower() == "qualy":
        session = "qualifying"
    elif session.lower() == "r" or session.lower() == "race":
        session = "race"
    output_root = get_property("App", "output_root")
    data_path = f"{output_root}/{year}/{race.lower()}/{session.lower()}"
    return Path(data_path)


def timedelta_to_seconds(df):
    # FastF1 provides timing data as timedelta[ns] fields.
    # The problem with timedelta[ns] is that it is not represented correctly in Arrow tables
    for col in df.select_dtypes(include=['timedelta64[ns]']).columns:
        df[col] = df[col].dt.total_seconds()

    return df



if __name__ == '__main__':
    pass
    # session = get_session_data(2024, 'Monaco', 'Q')
    # laps = session.laps.pick_drivers(23)
    # tele = laps.get_telemetry()
    # session = get_session_data(2024, 'Monaco', 'Q')
    # laps = session.laps.pick_drivers('VER')
    # telemetry = get_all_telemetry(laps)
    # write_parquet(telemetry, "telemetry_data.parquet")
