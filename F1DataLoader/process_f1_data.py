import fastf1
import pandas as pd
from F1DataLoader.parquet_utils import write_parquet
from F1DataLoader.duckdb_utils import create_duckdb_table
from F1DataLoader.f1_data_access import get_session_data
import os
from pathlib import Path
import config


def get_available_tables():
    data_root = Path(config.data_root)
    print(data_root)


def annotate_telemetry_with_lap_number(laps: fastf1.core.Laps):
    laps = laps.reset_index(drop=True)
    laps.index += 1
    for index, each_lap in laps.iterrows():
        each_lap.telemetry["LapNumber"] = each_lap["LapNumber"]
    return laps


def write_session_data(year, race, session):
    # TODO: need to clean this up, passing same variables around
    session_data = get_session_data(year, race, session)
    write_laps_data_for_session(year, race, session, session_data)
    write_telemetry_for_session(year, race, session, session_data)
    data_path = get_data_path(year, race, session)
    # TODO: need to find a way to not reconstruct the db path again and again.
    db = f"{race.lower()}_{session}_{year}.duckdb"
    db_path = os.path.join(data_path, db)
    query = f"ATTACH '{db_path}' AS {race.lower()}_{session.lower()}_{year}"



def get_all_telemetry(laps):
    laps = laps.reset_index(drop=True)
    laps.index += 1
    #TODO: find a better way to do this, basically we just need to add the LapNumber to telemetry
    telemetry_list = []
    for index, each_lap in laps.iterrows():
        try:
            telemetry = each_lap.get_telemetry()
            telemetry["Driver"] = each_lap["Driver"]
            telemetry["DriverNumber"] = each_lap["Driver"]
            telemetry["LapNumber"] = index
            telemetry_list.append(telemetry)
        except Exception as e:
            print(f"Exception in getting telemetry for driver: {each_lap['Driver']}")
            continue

    telemetry_df = pd.concat(telemetry_list)
    return telemetry_df


def write_telemetry_for_session(year, race, session, session_data):
    drivers = session_data.drivers
    all_telemetry = []
    data_path = get_data_path(year, race, session)
    data_path.mkdir(parents=True, exist_ok=True)
    for driver in drivers:
        laps = session_data.laps.pick_drivers(driver)
        telemetry = get_all_telemetry(laps)
        # streamlit is unable to render timedelta[ns]. converting it to seconds
        telemetry['SessionTime'] = telemetry['SessionTime'].dt.total_seconds()
        telemetry['Time'] = telemetry['Time'].dt.total_seconds()
        telemetry["Driver"] = driver
        # telemetry_file = data_path.joinpath(f"{driver}_telemetry.parquet")
        # write_parquet(telemetry, telemetry_file)
        all_telemetry.append(telemetry)

    all_telemetry_df = pd.concat(all_telemetry)
    if session.lower() == "q" or session.lower() == "qualifying" or session.lower() == "qualy":
        session = "qualifying"
    elif session.lower() == "r" or session.lower() == "race":
        session = "race"
    all_telemetry_file = data_path.joinpath(f"telemetry_{session.lower()}_{year}.parquet")
    write_parquet(all_telemetry_df, all_telemetry_file)
    table_name = f"telemetry"
    db = f"{race.lower()}_{session.lower()}_{year}.duckdb"
    db_path = data_path.joinpath(f"{db}")
    create_duckdb_table(all_telemetry_df, table_name, str(db_path))
    print("done telemetry")


def write_laps_data_for_session(year, race, session, session_data):
    data_path = get_data_path(year, race, session)
    data_path.mkdir(parents=True, exist_ok=True)
    all_laps_file = data_path.joinpath(f"laps_{session.lower()}_{year}.parquet")
    laps_df = session_data.laps
    write_parquet(laps_df, all_laps_file)
    table_name = f"laps"
    db = f"{race.lower()}_{session.lower()}_{year}.duckdb"
    db_path = data_path.joinpath(f"{db}")
    create_duckdb_table(laps_df, table_name, str(db_path))
    print("done laps")


def get_data_path(year, race, session):
    if session.lower() == "q" or session.lower() == "qualifying" or session.lower() == "qualy":
        session = "qualifying"
    elif session.lower() == "r" or session.lower() == "race":
        session = "race"
    data_path = f"{config.data_root}/{year}/{race.lower()}/{session.lower()}"
    return Path(data_path)


if __name__ == '__main__':
    write_telemetry_for_session(2024, 'Mexico', 'Race')
    # session = get_session_data(2024, 'Monaco', 'Q')
    # laps = session.laps.pick_drivers(23)
    # tele = laps.get_telemetry()
    # session = get_session_data(2024, 'Monaco', 'Q')
    # laps = session.laps.pick_drivers('VER')
    # telemetry = get_all_telemetry(laps)
    # write_parquet(telemetry, "telemetry_data.parquet")
