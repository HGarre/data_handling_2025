# -*- coding: utf-8 -*-
"""
Created on Thu Jul  3 13:25:27 2025

@author: garre
"""

# TODO
# make unit change possible DONE
# include standard deviation #Bug: unit transformation is at wrong position
# try to have excel columns of output nicely formated
# make appending data possible (rows and cloumns)
# make it possible to use costum IDs in input data together with an extra table that related costum IDs to TRTNO

#Code to copy point-based data from a data sheet used for data input into a ICASA data template.
#The data file from where the data is copied needs to have a column 
#that specifies the ODMF number assigned to the point where the sample was taken.
#All columns that should be transferred need to have the same variable name as in the ICASA templete.



#provide the complete path to the input data sheet (excel format) and sheet name within the workbook
input_file = "H:/Data/phenology_height_2025.xlsx"
input_sheet = "Tabelle1"

#provide the complete path to the icasa template (excel format) and sheet name to which the data should be copied
template_file = "H:/Data/FORMULA_SP5_crop_measurement_3.xlsx"
template_sheet = "phenology"

#provide unit change information (optional)
#provide a dictionary of all variables that need a unit change and 
#the corresponding factors to tranform from the input unit to the output unit 
#(e.g.plant_height: input centimeter, output meter, provide "PHTD":0.01)

unit_change = {"PHTD":0.01}


#specify whether data should be summarized over samples (tecnical replicates on the same DATE), 
#in this case only columns containing numbers or time can be transferred
#a column "DATE" must be in the input data sheet

summarize_samples = True



import pandas as pd

# imporating the data

input_data = pd.read_excel(input_file, sheet_name = input_sheet)

template_data = pd.read_excel(template_file, sheet_name=template_sheet)

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

#export the icasa_data back into the excel file

common_cols_2 = input_data_subset.columns.intersection(template_data.columns) #needed to include standard deviation if intended

template_data[common_cols_2] = input_data_subset[common_cols_2]

with pd.ExcelWriter(template_file, mode='a', if_sheet_exists='replace') as writer:
    template_data.to_excel(writer, sheet_name=template_sheet, index=False)
    