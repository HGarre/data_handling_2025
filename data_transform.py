# -*- coding: utf-8 -*-
"""
Created on Thu Jul  3 13:25:27 2025

@author: garre
"""

# TODO
# DONE make unit change possible DONE
# DONE include standard deviation #Bug: unit transformation is at wrong position DONE
# DONE make appending data possible (rows and colums)
# DONE try to have excel columns of output nicely formated
# DONE make it possible to use costum IDs and variable names in input data together with an extra table (mapping) that related costum IDs to TRTNO and variable names to ICASA codes
# construct edge cases to test the script for robustness

#Code to copy point-based data from a data sheet used for data input into a ICASA data template.
#The data file from where the data is copied needs to have a column 
#that specifies the ODMF number assigned to the point where the sample was taken
#All columns that should be transferred need to have the same variable name as in the ICASA templete or a mapping must be provided
#if you run into problems due to datatypes of columns that are empty in one or the other data sheet, try inserting a dummy row with values of the correct type and delete it later

#provide the complete path to the input data sheet (excel format) and sheet name within the workbook
input_file = "H:/Data/phenology_height_2025.xlsx"
input_sheet = "all"

#provide the complete path to the icasa template (excel format) and sheet name to which the data should be copied
template_file = "H:/Data/FORMULA_SP5_crop_measurement_3.xlsx"
template_sheet = "plant_height"

# specify whether you want to provide a mapping table instead of using the ICASA variable names in your input_file
#if true, provide a mapping table that contains the ICASA varaible names in the first column and your variable names in the second column
# it can contain more variable names than used in the sheets you want to transform

use_mapping = False
mapping_file = ""
mapping_sheet = ""

# specify whether data should be summarized over tecnical replicates (RP) on the same DATE, 
# If you choose to summarize:
#   only columns containing numbers or time can be transferred
#   a column "DATE" must be in the input data sheet, replicates can have any name
#   standard deviation will be added if the column is included in the template
# if replicates are present but no summary is intended
#    a column RP must specify replicate numbers both in the input and the template file

summarize_samples = True

#provide unit change information (optional)
#provide a dictionary of all variables that need a unit change and 
#the corresponding factors to tranform from the input unit to the output unit 
#(e.g.plant_height: input centimeter, output meter, provide "PHTD":0.01)

unit_change = {}


#specify whether you want to overwrite existing values in the template_file with values from the input file
#this can be used if wrong values have been imported before. 
#Be aware out that if some (wrong) values are stored in template, they will not be overwritten by importing empty lines.
# In this case you need to delete the value manually. This is also true for previously calcualted standard deviations.
#Choose False if no data from the template file should be lost.

overwrite_values = False



import pandas as pd

# imporating the data

input_data = pd.read_excel(input_file, sheet_name = input_sheet)

template_data = pd.read_excel(template_file, sheet_name=template_sheet)


#rename input data columns

if use_mapping:
    mapping = pd.read_excel(mapping_file, sheet_name=mapping_sheet)
    rename_dict = dict(zip(mapping.iloc[:,1],mapping.iloc[:,0]))
    input_data = input_data.rename(columns=rename_dict)

#checking for common columns

common_cols = input_data.columns.intersection(template_data.columns)

#transforming time column if applicable, in order to be summarized

if summarize_samples and "TIME" in common_cols:
    input_data["TIME"] = pd.to_timedelta(input_data["TIME"].astype(str))

#subsetting data 

input_data_subset = input_data.loc[:,common_cols]

#transforming units

for entri in unit_change:
    input_data_subset[entri] = input_data_subset[entri]*unit_change[entri]

#summarizing data and computing standard deviation

if summarize_samples:
    input_data_subset = input_data_subset.groupby(["TRTNO", 'DATE']).agg(['mean', 'std']).reset_index() #reset index avoids merged cells for same TRTNO

    new_columns = []
    for col in input_data_subset.columns:
        if col[1] == '':  # This is a grouping column like ('TRTNO', '') or ('DATE', '')
            new_columns.append(col[0])
        elif col[1] == 'mean':
            new_columns.append(col[0])  # Keep original name
        elif col[1] == 'std':
            new_columns.append(col[0] + 'S')  # Append 'S' for std
    
    input_data_subset.columns = new_columns

common_cols_2 = input_data_subset.columns.intersection(template_data.columns) #needed to include standard deviation if intended and find correct keys

input_data_subset = input_data_subset[common_cols_2]

# unite the template and input data
if "DATE" and "RP" in common_cols_2:
    keys = ["TRTNO", "DATE", "RP"]
elif "RP" in common_cols_2:
    keys = ["TRTNO", "RP"]
elif "DATE" in common_cols_2:
    keys = ["TRTNO", "DATE"]
else:
    keys = ["TRTNO"]

data_cols = [col for col in common_cols_2 if col not in keys]

merged_data = pd.merge(template_data, input_data_subset, on = keys, how = 'outer', suffixes = ("_t", "_i"))

if overwrite_values:
    for col in data_cols:
        merged_data[col] = merged_data[f"{col}_i"].combine_first(merged_data[f"{col}_t"]) #creates combination columns that have the original names (stored in data_cols), containing value from input, only if input has no value, use value from template
else:
    for col in data_cols:
        merged_data[col] = merged_data[f"{col}_t"].combine_first(merged_data[f"{col}_i"]) #creates combination columns that have the original names (stored in data_cols), containing value from template, only if template was no value, use value from input
    
final_data = merged_data[template_data.columns] #merged data contains combination column and the columns with indexes, keep only keys and combination columns and empty columns from template sheet

# write the new template into the old excel sheet (and format the columns)

with pd.ExcelWriter(template_file, mode='a', if_sheet_exists='replace', engine = "openpyxl") as writer:
    final_data.to_excel(writer, sheet_name=template_sheet, index=False)
    
    wb  = writer.book
    ws = writer.sheets[template_sheet]
    
    header = [cell.value for cell in ws[1]]
    
    
    if "DATE" in common_cols_2:
        date_col_idx = header.index("DATE") + 1 # openpyxl columns are 1-based
        for row in ws.iter_rows(min_row=2, min_col=date_col_idx, max_col=date_col_idx):
            row[0].number_format = "yyyy-mm-dd"
    
    if "TIME" in common_cols_2:
        time_col_idx = header.index("TIME") + 1
        for row in ws.iter_rows(min_row=2, min_col=time_col_idx, max_col=time_col_idx):
            row[0].number_format = "hh:mm:ss"
        