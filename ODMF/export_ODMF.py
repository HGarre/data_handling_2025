# -*- coding: utf-8 -*-
"""
Created on Mon Oct  6 16:25:49 2025
Revised: Added config file for credentials

@author: garre
"""

# FORMULA project id: 7

import configparser
from odmfclient import login
import pandas as pd

config_path = "config.ini"
config = configparser.ConfigParser()
config.read(config_path)

url = config["odmf"]["url"]
username = config["odmf"]["username"]
password = config["odmf"]["password"]


    
def data_by_valuetype(api, valuetype_id, project_id):    
    datasets = api.dataset.list(valuetype=valuetype_id, project=project_id)
    data_total = pd.DataFrame(columns=["time", "value", "site", "level"])
    for dataset_id in datasets:
        data = api.dataset.values_parquet(dsid=dataset_id)
        dataset_obj = api.dataset(dsid=dataset_id)
        site = dataset_obj["site"]["id"]
        data["site"]=site
        level = dataset_obj["level"]
        data["level"]=level
        print(data.head())
        data_total = pd.concat([data_total, data], ignore_index = True)
    return data_total

with login(url, username, password) as api:
    data_soil_moisture = data_by_valuetype(api, 10, 7)
    #datasets= api.dataset.list(valuetype=10, project=7)
    #data_example = api.dataset.values_parquet(dsid=3098)
    #data_obj_example = api.dataset(dsid=3098)