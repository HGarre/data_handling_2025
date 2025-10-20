# -*- coding: utf-8 -*-
"""
Created on Mon Oct 20 12:05:35 2025

@author: garre
"""

import pandas as pd
#from pathlib import Path
#import argparse
#import sys


def extract_two_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Helper: keep only rows 3 and 4 (index 2 & 3) and drop columns that are empty
    in **both** rows. Return a DataFrame of shape (n_filled_cells, 2)
    where column 0 = row‑3 value, column 1 = row‑4 value.
    """
    rows = df.iloc[[2, 3]]

    transposed = rows.T.reset_index(drop=True)
    transposed.columns = ["Unit_or_type", "Variable_name"]          
    return transposed


def build_glossary_dataframe(
    src_path: str,
    row_indices: tuple[int, int] = (2, 3)
) -> pd.DataFrame:
    """
    Reads the given excel file and extracts the given columns (index starts at  0) from each sheet.

    The output has three columns:
        - Row3 : value from Excel row 3
        - Row4 : value from Excel row 4
        - Sheet: name of the sheet the pair came from
    """
    sheet_dict = pd.read_excel(
            src_path,
            sheet_name=None,
            header=None,
            dtype=str,
            engine="openpyxl"
        )

    all_blocks = []                         

    for sheet_name, raw_df in sheet_dict.items(): #looping through the dictionary and keeping both the keys (assigned to sheet_name) and the values (in this case dataframes assigned to raw_df)
        
        sliced_df = raw_df.iloc[:max(row_indices)+1]   # safe cut‑off to spare memory

        block = extract_two_rows(sliced_df)
        
        block["Sheet"] = sheet_name          
        # Keep the column order that the final CSV expects
        block = block[["Sheet", "Variable_name", "Unit_or_type",]]
        all_blocks.append(block)

    glossary_df = pd.concat(all_blocks, axis=0, ignore_index=True)

    return glossary_df

'''
def write_glossary_to_new_file(
    glossary_df: pd.DataFrame,
    dest_path: Path,
    sheet_name: str = "glossary"
) -> None:
    """
    Writes *glossary_df* to a brand‑new workbook *dest_path*.
    If the file already exists it will be overwritten.
    """
    # ExcelWriter with engine=openpyxl creates a fresh file when mode='w'
    with pd.ExcelWriter(dest_path, engine="openpyxl", mode="w") as writer:
        glossary_df.to_excel(writer, sheet_name=sheet_name, index=False)
    print(f"[DONE] Glossary saved to '{dest_path}' (sheet name: '{sheet_name}').")
'''

if __name__ == "__main__":
    glossary = build_glossary_dataframe("C:/Users/garre/Data/ICASA_for_agroforstry_draft_4.xlsx", (2,3))