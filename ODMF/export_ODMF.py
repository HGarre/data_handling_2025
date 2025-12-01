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
import re


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
    .rename(columns={'time': 'date', 'value': 'value_agg'})
    )
    return data_summed

def extract_ICASA_info (api, valuetype_id, project_id):
    '''
    Extracts information about the ICASA variable corresponding to the given value_type.
    For now only the first ICASA variable_name is extracted.

    Parameters
    ----------
    api : ?
        Odmfclient login with url, username and password. Make sure that you have access to the data you want to export.
    valuetype_id : int
        ID given in the ODMF system to tcdhe valuetype for which the information should be extracted.
    project_id : int
        ID of a project in ODMF that uses the valuetype to ensure access to the information via datasets.

    Returns
    -------
    ICASA_dict : dictionary
        dictionary containing the ICASA variable name, the unit conversion factor 
        and the agrregation function for transform from the value_type to the ICASA variable_name.
        
    '''
    datasets = api.dataset.list(valuetype=valuetype_id, project=project_id)
    first_dataset = datasets[1]
    first_dataset_obj = api.dataset(dsid=first_dataset)
    valuetype_info =first_dataset_obj["valuetype"]["comment"]
   
    pattern = re.compile(
    r'''
    ICASA:\s*
    (?P<Variable_name>[^*\n,]+)          # Variable_name (mandatory)
    (?:\*(?P<conversion>\d+(?:\.\d+)?))?   # *conversion (optional but required if * present)
    (?:,\s*(?P<aggregation>\S+))?          # ,aggregation (optional but required if , present)
    \s*$                        # line end
    ''',
    re.VERBOSE | re.MULTILINE
    )
   
    extraction=pattern.search(valuetype_info)
    ICASA_dict=extraction.groupdict()
   
    factor = ICASA_dict.get("conversion")
    ICASA_dict["conversion"]=float(factor) if factor else None
   
    return ICASA_dict

def data_to_ICASA_by_valuetype (api, valuetype_id, project_id, start_date, end_date, file_name, level_col=False):
    '''
    

    Parameters
    ----------
    api : ?
        Odmfclient login with url, username and password. Make sure that you have access to the data you want to export.
    valuetype_id : Integer
        ID given in the ODMF system to the valuetype for which data should be exported.
    project_id : Integer
        ID given in the ODMF system to the project from which data should be exported.
        DESCRIPTION.
    start_date : String
        First date for which data should be exported in format yyyy-mm-dd.
    end_date : String
        Last date for which data should be exported in format yyyy-mm-dd.
    file_name: string
        Name of the ICASA template file into which the data should be pasted.
    level_col: string, optional
        Optional. If applicable, name of the column in the ICASA sheet corresponsing to the ODMF levels (e.g. soil layers).

    Returns
    -------
    None.

    '''
    ICASA_info = extract_ICASA_info(api, valuetype_id, project_id)
    ICASA_name = ICASA_info["Variable_name"]
    #search for ICASA name within the file, if not found retrun error
    #if found, check whether there is the given level column, if not return error
    #fetch the data
    #check whether the data contains layers, if not match what is provided to funtion raise error
        #check whether helper functions work for data without levels, make more felxible if needed
    return ICASA_name
    
with login(url, username, password) as api:
    #data_soil_moisture = data_by_valuetype(api, 10, 7, "2025-10-10", "2025-10-12")
    #ICASA_soil_moisture = extract_ICASA_info(api, 10, 7)
    #agg = ICASA_soil_moisture.get("aggregation")
   # data_soil_moisture_agg = agg_data_daily(data_soil_moisture, agg)
    ICASA_test_output = data_to_ICASA_by_valuetype(api, 10, 7, "2025-10-10", "2025-10-12", "test-file")
    
    #datasets_example = api.dataset.list(valuetype=10, project=7)
    #data_example = api.dataset.values_parquet(dsid=3098, start="2025-10-10", end="2025-10-13")
    #data_obj_example = api.dataset(dsid=3098)
    #valuetype_obj_example = api.dataset.listobj(valuetype=10) #reveals many dataset objects, not the valuetype object
    