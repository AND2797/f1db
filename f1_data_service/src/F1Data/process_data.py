import pandas as pd
from pathlib import Path
from f1_data_service.src.F1Data.access_data import F1DataRequest
from f1_data_service.src.app_utils.config_utils import get_property


class F1DataProcessor:
    """
    This class is meant to encompass the processing of a single F1DataRequest object
    It uses the F1DataRequest object to figure out where the parquet files need to be written to
    """

    def __init__(self, data_request: F1DataRequest, pqf, db):
        self.data_request = data_request
        self.session_data = self.data_request.session_data
        self.duckdb = db
        self._pqf = pqf
        self.data_root = self._construct_path(self.data_request)

    def _get_laps(self):
        # helper to get laps?
        pass

    def write_session_data(self):
        self.write_laps()
        self.write_telemetry_by_driver()

    def write_laps(self):
        # we need to create a copy because in .post_process_laps we modify the column names
        # changing the column names to lower case or snake case breaks FastF1 methods
        laps = self.session_data.laps.copy()
        laps = self.post_process_laps(laps)

        self._pqf.write(laps, self.data_root.joinpath("laps.parquet"))
        # self.duckdb.create_table(path, laps, "laps")

    def write_telemetry_by_driver(self):
        drivers = self.session_data.laps['Driver'].unique()
        self.data_root.joinpath("telemetry").mkdir(parents=True, exist_ok=True)
        for driver in drivers:
            laps = self.session_data.laps.pick_drivers(driver)
            telemetry = self.get_all_telemetry(laps)
            telemetry['year'] = self.data_request.year
            telemetry['race'] = self.data_request.race
            telemetry['session'] = self.data_request.session
            telemetry.columns = (telemetry.columns
                                 .str.replace('(?<=[a-z])(?=[A-Z])', '_', regex=True)
                                 .str.lower())
            telemetry = timedel_to_seconds(telemetry)
            self._pqf.write(telemetry, f"{self.data_root}/telemetry/{driver}.parquet")

    def post_process_laps(self, laps: pd.DataFrame):
        laps['year'] = self.data_request.year
        laps['race'] = self.data_request.race
        laps['session'] = self.data_request.session

        laps.columns = (laps.columns
                           .str.replace('(?<=[a-z])(?=[A-Z])', '_', regex=True)
                           .str.lower())

        laps = timedel_to_seconds(laps)
        return laps

    def write_telemetry(self, path):
        # TODO: Implement this
        df = self.combine_telemetry()
        self._pqf.write(path, df)
        self.duckdb.create_table(path, df, "telemetry")

        # TODO: think about whether data needs to be interpolated
        # interp_df = self.interpolate_telemetry_data(df)
        # self._pqf.write(path, df)
        # self._db.create_table(path, df, "telemetry")
        # self._db.create_table(path, interp_df, "telemetry")


    # def combine_telemetry(self):
    #     if self.session is None:
    #         return pd.DataFrame()
    #
    #     drivers = self.session.drivers
    #     combined_telemetry = []
    #     for driver in drivers:
    #         laps = self.session.laps.pick_drivers(driver)
    #         telemetry = self.get_all_telemetry(laps)
    #
    #         telemetry['year'] = self.session.year
    #         telemetry['race'] = self.session.race.lower()
    #         telemetry['session'] = self.session
    #         telemetry.columns = (telemetry.columns
    #                          .str.replace('(?<=[a-z])(?=[A-Z])', '_', regex=True)
    #                          .str.lower())
    #         telemetry = timedel_to_seconds(telemetry)
    #         combined_telemetry.append(telemetry)
    #
    #     combined_df = pd.concat(combined_telemetry)
    #     return combined_df

    def get_all_telemetry(self, laps):
        telemetry_list = []
        for index, lap in laps.iterrows():
            telemetry = lap.get_telemetry()
            telemetry["Driver"] = lap["Driver"]
            telemetry["DriverNumber"] = lap["DriverNumber"]
            telemetry["LapNumber"] = lap["LapNumber"]
            telemetry_list.append(telemetry)

        telemetry = pd.concat(telemetry_list)
        return telemetry

    def _construct_path(self, f1dr: F1DataRequest):
        base_path = Path(get_property("App", "data_root"))
        year = self.data_request.year
        race = self.data_request.race.lower()
        session = self.data_request.session.lower()
        session_path = base_path.joinpath(f"{year}", f"{race}", f"{session}")
        session_path.mkdir(parents=True, exist_ok=True)
        session_path.joinpath('telemetry').mkdir(parents=True, exist_ok=True)
        return session_path


def timedel_to_seconds(df):
    for col in df.select_dtypes(include=['timedelta64[ns]']).columns:
        df[col] = df[col].dt.total_seconds()
    return df










