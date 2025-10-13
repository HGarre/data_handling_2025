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
import numpy as np

config_path = "config.ini"
config = configparser.ConfigParser()
config.read(config_path)

url = config["odmf"]["url"]
username = config["odmf"]["username"]
password = config["odmf"]["password"]


    
def data_by_valuetype(api, valuetype_id, project_id, start_date, end_date): 
    """
    Exports all data from a given ODMF database and a given project that stores 
    values of the given type, between the start date and the end date (included).

    Parameters
    ----------
    api : ?
        Odmfclient login with url, username and password. Make sure that you have access to the data you want to export.
    valuetype_id : Integer
        ID given in the ODMF system to the valuetype for which data should be exported.
    project_id : Integer
        ID given in the ODMF system to the project from which data should be exported.
    start_date : String
        First date for which data should be exported in format yyyy-mm-dd.
    end_date : String
        Last date for which data should be exported in format yyyy-mm-dd.

    Returns
    -------
    data_total : DataFrame
        extracted data sorted by site, level, and time

    """
    datasets = api.dataset.list(valuetype=valuetype_id, project=project_id)
    data_total = pd.DataFrame(columns=["time", "value", "site", "level"])
    end_time = end_date+"T23:59:59Z"
    for dataset_id in datasets:
        data = api.dataset.values_parquet(dsid=dataset_id, start=start_date, end=end_time)
        dataset_obj = api.dataset(dsid=dataset_id)
        site = dataset_obj["site"]["id"]
        data["site"]=site
        level = dataset_obj["level"]
        data["level"]=level
        data_total = pd.concat([data_total, data], ignore_index = True)
    return data_total

def agg_data_daily(df, function_name):
    """
    Aggregates data exported from ODMF e.g. by data_by_valuetype per day using the given aggregation function.

    Parameters
    ----------
    df : Data.Frame
        A dataframe containing values sorted by site, level and time.
    function_name : string
        A aggregation function auch as mean, sum, min, max.

    Returns
    -------
    data_summed : TYPE
        DESCRIPTION.

    """
    data_summed = (
    df
    .groupby(
        [
            pd.Grouper(key='time', freq='D'),   # daily bucket, anchored at midnight UTC
            'site',
            'level'
        ]
    )['value']                                
    .agg(function_name)                                    
    .reset_index()                             
    .rename(columns={'time': 'date', 'value': 'value_mean'})
    )
    return data_summed

#def extraxt_ICASA_info (valuetype_id):
    
#    return ICASA_dict


with login(url, username, password) as api:
    data_soil_moisture = data_by_valuetype(api, 10, 7, "2025-10-10", "2025-10-12")
    data_soil_moisture_mean = agg_data_daily(data_soil_moisture, "mean")
    #datasets= api.dataset.list(valuetype=10, project=7)
    #data_example = api.dataset.values_parquet(dsid=3098, start="2025-10-10", end="2025-10-13")
    #data_obj_example = api.dataset(dsid=3098)