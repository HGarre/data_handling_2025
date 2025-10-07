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

if not os.path.exists(config_path):
    raise FileNotFoundError(f"Config file not found: {config_path}")

config = configparser.ConfigParser()
config.read(config_path)


# --- Read configuration ---
config = configparser.ConfigParser()
config.read(config_path)

url = config["odmf"]["url"]
username = config["odmf"]["username"]
password = config["odmf"]["password"]

# --- ODMF API login and data retrieval ---
with login(url, username, password) as api:
    datasets_soil_m = api.dataset.list(valuetype=10)
    data_soil_m = api.dataset.values_parquet(dsid=3146)

    # Example: print dataset info
    print("Datasets:", datasets_soil_m[:3])  # show first 3 for preview
    print("Data loaded for dsid=12345")



# # -*- coding: utf-8 -*-
# """
# Created on Mon Oct  6 16:25:49 2025
#
# @author: garre
# """
#
# from odmfclient import login
#
# with login("https://data.fb09.uni-giessen.de/gbh/", "helene.garre", "") as api:
#     datasets_soil_m = api.dataset.list(valuetype=10)
#     data_soil_m = api.dataset.values_parquet(dsid=3146)

