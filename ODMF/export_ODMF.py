# -*- coding: utf-8 -*-
"""
Created on Mon Oct  6 16:25:49 2025
Revised: Added config file for credentials

@author: garre
"""
import os
import configparser
from odmfclient import login

config_path = "config.ini"
config = configparser.ConfigParser()
config.read(config_path)

url = config["odmf"]["url"]
username = config["odmf"]["username"]
password = config["odmf"]["password"]

with login(url, username, password) as api:
    datasets_soil_m = api.dataset.listobj(valuetype=10)
    data_soil_m = api.dataset.values_parquet(dsid=3146)
    print(datasets_soil_m[1]["site"]["id"])
    one_dataset_obj = api.dataset(dsid=3146)
    print(one_dataset_obj["site"]["id"])
