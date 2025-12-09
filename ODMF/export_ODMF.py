# -*- coding: utf-8 -*-
"""
Created on Mon Oct  6 16:25:49 2025
Revised: Added config file for credentials

@author: garre
"""

# FORMULA project id: 7

import os
import configparser
from odmfclient import login
import pandas as pd
import re
from openpyxl import load_workbook
from openpyxl.utils.dataframe import dataframe_to_rows

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
    valuetype_id : integer
        ID given in the ODMF system to the valuetype for which data should be exported.
    project_id : integer
        ID given in the ODMF system to the project from which data should be exported.
    start_date : string
        First date for which data should be exported in format yyyy-mm-dd.
    end_date : string
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
    df["level"] = df["level"].fillna("None") #makes None a string to make grouping possible if there are no levels in the data
    
    data_summed = (
    df
    .groupby(
        [
            pd.Grouper(key='time', freq='D'),
            'site',
            'level'
        ]
    )['value']                                
    .agg(function_name)                                    
    .reset_index()                             
    .rename(columns={'time': 'date'})
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
    valuetype_id : integer
        ID given in the ODMF system to tcdhe valuetype for which the information should be extracted.
    project_id : integer
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

def find_ICASA_sheet_by_variable_name (variable_name, file_path):
    '''
    

    Parameters
    ----------
    variable_name : string
        Name of the ICASA variable to localize.
    file_path : string
        Path to the ICASA template to search in.

    Returns
    -------
    sheet_name: str

    '''
    wb = load_workbook(file_path, data_only=True)

    for sheet in wb.sheetnames:
        ws = wb[sheet]
        row4_values = [cell.value for cell in ws[4]]
        if any(str(value) == str(variable_name) for value in row4_values if value is not None):
            sheet_name = sheet
    
    if "sheet_name" not in locals():
        raise ValueError("Variable name is not found in the provided ICASA template")
    
    return sheet_name
            
def merge_new_data_to_ICASA (new_data, template_data, level_col = None, overwrite=False):
    '''

    Parameters
    ----------
    new_data : dataframe
        Containing the data to add to the template.
    template_data : dataframe
        Containing the sheet of the template to which the new data should be added.
    level_col: string, optional
        Name of the column in the ICASA template into which ODMF layer information should be pasted (e.g. me_soil_layer_bot_depth)
    overwrite : boolean, optional
        Switch to allow overwriting existing values in the ICASA template with new data. The default is False.

    Returns
    -------
    None.

    '''
    common_cols = new_data.columns.intersection(template_data.columns)
    new_data_subset = new_data.loc[:,common_cols]
    
    if "date_of_measurement" and "time_of_measurement" and level_col in common_cols:
        keys=["date_of_measurement", "time_of_measurement", level_col]
    elif "date_of_measurement" and level_col in common_cols:
        keys=["date_of_measurement", level_col]
    elif "date_of_measurement" and "time_of_measurement" in common_cols:
        keys=["date_of_measurement", "time_of_measurement"]
    else:
        keys=["date_of_measurement"]

def data_to_ICASA_by_valuetype (api, valuetype_id, project_id, start_date, end_date, file_path, level_col = None, overwrite =False):
    '''
    

    Parameters
    ----------
    api : ?
        Odmfclient login with url, username and password. Make sure that you have access to the data you want to export.
    valuetype_id : integer
        ID given in the ODMF system to the valuetype for which data should be exported.
    project_id : integer
        ID given in the ODMF system to the project from which data should be exported.
        DESCRIPTION.
    start_date : string
        First date for which data should be exported in format yyyy-mm-dd.
    end_date : String
        Last date for which data should be exported in format yyyy-mm-dd.
    file_path: string
        Path to the ICASA template file into which the data should be pasted.
    level_col: string, optional
        Name of the column in the ICASA template into which ODMF layer information should be pasted (e.g. me_soil_layer_bot_depth)
    overwrite: boolean, optional
        Switch to allow overwriting existing values in the ICASA template with new data. The default is False.
    Returns
    -------
    None.

    '''
    try: 
        ICASA_info = extract_ICASA_info(api, valuetype_id, project_id)
    except:
        raise ValueError("No ICASA info or datasets can be found for the given valuetype. Check whether (1) The given valuetype has an ICASA comment in ODMF, (2) datasets are present for the given valuetype and you have access to them via the project and api provided, and (3) you are connected to a network that gives you access to ODMF.")
        
    ICASA_name = ICASA_info["Variable_name"]
    ICASA_conversion = ICASA_info["conversion"]
    ICASA_aggregation = ICASA_info["aggregation"]
    
    data = data_by_valuetype(api, valuetype_id, project_id, start_date, end_date)
    
    if data.empty:
        raise ValueError("No datasets could be exported. Check whether (1) Datasets are present for the given valuetype and you have access to them via the project and api provided, (2) the datsets have entries in the time span you provided, and (3) you are connected to a network that gives you access to ODMF.")
    
    if ICASA_conversion != None:
       data["value"] = data["value"]/ICASA_conversion
    
    if ICASA_aggregation != None:
        data = agg_data_daily(data, ICASA_aggregation)
    else:
        data = data.rename(columns={"time": "date_of_measurement"})
    
    if level_col == None:
        data.drop("level", axis=1, inplace=True)
    else: 
       data = data.rename(columns={"level": level_col})
        
    data = data.rename(columns={"date": "date_of_measurement", "site": "sampling_location_number", "value": ICASA_name})
        
    ICASA_sheet_name = find_ICASA_sheet_by_variable_name(ICASA_name, file_path)
    
    template_data = pd.read_excel(file_path, sheet_name=ICASA_sheet_name, skiprows=3)
    
    
    
    #adapt from data_transform (evetually built more helper functions)
        
    
    return data
    

input_file = "ICASA_for_agroforstry_draft_final.xlsx"
output_file = ""
BASE_DIR = os.path.abspath(os.path.dirname(__file__)) #do not run this line alone, only works when entire scrip is run
input_path = os.path.join(BASE_DIR, input_file)
output_path = os.path.join(BASE_DIR, output_file)

#Sheet_name_test = find_ICASA_sheet_by_variable_name("soil_water_by_layer", input_path)

with login(url, username, password) as api:
    
    #data_LAI = data_by_valuetype(api, 34, 7, "2025-04-04", "2025-07-01")
    #ICASA_soil_moisture = extract_ICASA_info(api, 10, 7)
    #agg = ICASA_soil_moisture.get("aggregation")
    #data_LAI_agg = agg_data_daily(data_LAI, "mean")
    ICASA_test_output = data_to_ICASA_by_valuetype(api, valuetype_id=10, project_id=7, start_date="2025-10-15", end_date="2025-10_20", file_path=input_file, level_col = "me_soil_layer_top_depth")
    
    #datasets_example = api.dataset.list(valuetype=10, project=7)
    #data_example = api.dataset.values_parquet(dsid=3098, start="2025-10-10", end="2025-10-13")
    #data_obj_example = api.dataset(dsid=3098)
    #valuetype_obj_example = api.dataset.listobj(valuetype=10) #reveals many dataset objects, not the valuetype object
    