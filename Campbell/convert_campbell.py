"""
Created on  May 25 2026

@author: Helene Garre

This Skript can be used to import data downloaded from Campbell Scientific data loggers into the ODMF. 
It cleanes unneccessary columns and converts the data into a long format with columns "time", "dataset_id", and "value"
as required for ODMF record import. The mapping of datalogger channel names to dataset IDs is done using an Excel file. 
The resulting DataFrame can be saved as a CSV file or directly imported into ODMF using the API.
"""

import pandas as pd
import os
import yaml
from odmfclient import login


def convert_campbell_LED_to_ODMF_record(data_path, datasetmap, datalogger, starttime, endtime):
    '''
    Convert Campbell Scientific LED voltage data to a long format suitable for ODMF.

    Parameters:
    - data_path: Path to the data as downloaded from Campbell Scientific data logger (e.g., "CR300Series_Minutentabelle.dat").
    - datasetmap: Path to the Excel file containing the mapping of channel names to dataset IDs (e.g., "LED_radiation_sensors.xlsx").
    - datalogger: The name of the datalogger as in the datasetmap (e.g., "T2").
    - starttime: The start time for filtering the data (e.g. after ssetup of sensors is finished).
    - endtime: The end time for filtering the data (e.g. before next sensor adjustment).
    
    Returns:    
    - A pandas DataFrame with colums "time", "dataset_id", and "value" as required for ODMF record import. 
    '''

    data = pd.read_csv(data_path, sep=',', header=0, skiprows=[0, 2, 3], na_values="NAN")
    LEDvolt_cols = [f"SEVolt_Avg({i})" for i in range(1, 13)]
    LEDvolt_data = data[["TIMESTAMP", *LEDvolt_cols]]

    LEDvolt_data["time"] = pd.to_datetime(LEDvolt_data["TIMESTAMP"], format="%Y-%m-%d %H:%M:%S")
    LEDvolt_data = LEDvolt_data.drop(columns=["TIMESTAMP"])

    start = pd.to_datetime(starttime)
    end = pd.to_datetime(endtime)
    LEDvolt_data = LEDvolt_data[(LEDvolt_data["time"] >= start) & (LEDvolt_data["time"] <= end)].reset_index(drop=True)

    map = pd.read_excel(datasetmap)
    filtered_map = map[map["Datalogger"] == datalogger]
    map_dict = dict(zip(filtered_map["Channel_Name"], filtered_map["dataset_id"]))
    LEDvolt_data = LEDvolt_data.rename(columns=map_dict)

    LEDvolt_long = LEDvolt_data.melt(id_vars=["time"], var_name="dataset_id", value_name="value")

    LEDvolt_long["dataset_id"] = LEDvolt_long["dataset_id"].astype(int)

    return LEDvolt_long

if __name__ == "__main__":

    project_dir = os.path.abspath(os.path.dirname(__file__))
    T2_data_path = os.path.join(project_dir, 'CR300Series_Minutentabelle.dat')
    datasetmap_path = os.path.join(project_dir, 'LED_radiation_sensors.xlsx')
    main_dir = os.path.join(project_dir, "../ODMF")
    
    config_path = os.path.join(main_dir, "config.yaml")
    with open(config_path, "r", encoding="utf-8") as cf:
        cfg = yaml.safe_load(cf)

    odmf_cfg = cfg.get("odmf", cfg)

    url = odmf_cfg["url"]
    username = odmf_cfg["username"]
    password = odmf_cfg["password"]

    T2_LED_log =convert_campbell_LED_to_ODMF_record(datalogger = "T2", data_path = T2_data_path, starttime = "2026-05-14 10:00:00", endtime = "2026-05-21 12:00:00", datasetmap = datasetmap_path)

    T2_LED_log.to_csv(os.path.join(project_dir, 'T2_LED_log.csv'), index=False)
    '''
    with login('https://path/to/odmf', 'user', 'password') as api:
        api.dataset.add_records_parquet(T2_LED_long)
    '''
