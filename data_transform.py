# -*- coding: utf-8 -*-
"""
Created on Thu Jul  3 13:25:27 2025

@author: garre
"""

import pandas as pd

# imporating the sheet with data, extracting the most important columns and computing mean and standard deviation

lai_2025 = pd.read_excel("H:/Data/LAI_2025.xlsx")
#lai_2025["TIME"] = pd.to_datetime(lai_2025["TIME"], format="%H:%M:%S").dt.time # .dt.time to extract the time and avoid that a dummy date is attached. However still dtype object afterwards.
lai_2025["TIME"] = pd.to_timedelta(lai_2025["TIME"].astype(str)) # to_timedelta does not except objects
                                   
lai_2025_subset = lai_2025[["TRTNO","DATE", "TIME", "LAI"]]

lai_2025_summary = lai_2025_subset.groupby(["TRTNO", 'DATE']).mean().reset_index() #reset index avoids merged cells for same TRTNO

# importing the corresponding data sheet of the icasa template and filling it with the values

icasa_lai = pd.read_excel("H:/Data/FORMULA_SP5_crop_measurement_2.xlsx", sheet_name="LAI")

# Find common columns
common_cols_LAI = lai_2025_summary.columns.intersection(icasa_lai.columns)

icasa_lai[common_cols_LAI] = lai_2025_summary[common_cols_LAI]

#export the icasa_data back into the excel file

with pd.ExcelWriter("H:/Data/FORMULA_SP5_crop_measurement_3.xlsx", mode='a', if_sheet_exists='replace') as writer:
    icasa_lai.to_excel(writer, sheet_name='LAI', index=False)
    