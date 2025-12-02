from pyspark.sql import SparkSession
import os
from pathlib import Path
from pyspark.sql.functions import col


catalog_name = "f1db"
telemetry_table_prefix = "f1db.telemetry"
laps_table_prefix = "f1db.laps"
data_root = "/Users/aditya/Projects/projects_data/f1db"

def get_parquet_file_paths(data_root):
    available_years = os.listdir(data_root)
    telemetry_files_dictionary = {}
    for year in available_years:
        year_dir = os.path.join(data_root, year)
        telemetry_files_dictionary[year] = dict()
        for race in Path(year_dir).iterdir():
            telemetry_files_dictionary[year][race.name] = dict()
            for session in Path(race).iterdir():
                telemetry_files_dictionary[year][race.name][session.name] = list(Path(session).joinpath('telemetry').iterdir())

    return telemetry_files_dictionary

def create_or_insert_table(files, table_name):
    #TODO: fill out this function
    for file in files:
        parquet_df = spark.read.parquet(file)
        try:
            parquet_df.write.format("iceberg")


def main():

    file_paths = get_parquet_file_paths(data_root)

    ICEBERG_SPARK_CONFIG = {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.f1db": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.f1db.type": "hadoop",
        "spark.sql.catalog.f1db.warehouse": f"{data_root}/iceberg_warehouse" # Local path for testing
    }

    builder = SparkSession.builder.appName("f1db")

    for key, value in ICEBERG_SPARK_CONFIG.items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    for race in file_paths.keys():
        for session, files in file_paths[race].keys():
            table_name = f"{telemetry_table_prefix}.{race}"
            create_or_insert_table(files, table_name)



if __name__ == '__main__':
    print(get_parquet_file_paths(data_root))
