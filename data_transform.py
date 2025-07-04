# -*- coding: utf-8 -*-
"""
Created on Thu Jul  3 13:25:27 2025

@author: garre
"""
#Code copy data from a data sheet used for data input into a ICASA data template
#The data file from where the data is copied from needs to have a column 
#that specifies the ODMF number assigned to the point where the sample was taken
#All columns that should be transferred need to have the same name as in the ICASA templete

import pandas as pd

#provide the complete path to the input data sheet (excel format) and sheet name within the workbook
input_file = "H:/Data/LAI_2025.xlsx"
input_sheet = "all"

#provide the complete path to the icasa template (excel format) and sheet name to which the data should be copied
template_file = "H:/Data/FORMULA_SP5_crop_measurement_3.xlsx"
template_sheet = "LAI"

#

#specify whether data should be summarized over samples (tecnical replicates on the same DATE), 
#in this case only columns containing numbers or time can be transferred
#a column "DATE" must be in the input data sheet

summarize_samples = True

# imporating the data

input_data = pd.read_excel(input_file, sheet_name = input_sheet)

template_data = pd.read_excel(template_file, sheet_name=template_sheet)

#checking for common columns

common_cols = input_data.columns.intersection(template_data.columns)

#transforming time column if applicable, in order to be summarized

if summarize_samples and "TIME" in common_cols:
    input_data["TIME"] = pd.to_timedelta(input_data["TIME"].astype(str))

#subsetting and summarizing data

input_data_subset = input_data[common_cols]

if summarize_samples:
    input_data_summary = input_data_subset.groupby(["TRTNO", 'DATE']).mean().reset_index() #reset index avoids merged cells for same TRTNO

template_data[common_cols] = input_data_summary[common_cols]

#export the icasa_data back into the excel file

with pd.ExcelWriter(template_file, mode='a', if_sheet_exists='replace') as writer:
    template_data.to_excel(writer, sheet_name=template_sheet, index=False)
    