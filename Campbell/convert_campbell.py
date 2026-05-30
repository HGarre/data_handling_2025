import pandas as pd
import os

project_dir = os.path.abspath(os.path.dirname(__file__))
T2_data_path = os.path.join(project_dir, 'CR300Series_Minutentabelle.dat')
datasetmap_path = os.path.join(project_dir, 'LED_radiation_sensors.xlsx')

def convert_campbell_LED_to_ODMF_record(datalogger, data_path, starttime, endtime, datasetmap):

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
    T2_LED_log =convert_campbell_LED_to_ODMF_record(datalogger = "T2", data_path = T2_data_path, starttime = "2026-05-14 10:00:00", endtime = "2026-05-21 12:00:00", datasetmap = datasetmap_path)

    T2_LED_log.to_csv(os.path.join(project_dir, 'T2_LED_log.csv'), index=False)
