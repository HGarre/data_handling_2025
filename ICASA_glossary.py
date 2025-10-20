# -*- coding: utf-8 -*-
"""
Created on Mon Oct 20 12:05:35 2025

@author: garre
"""

import pandas as pd
import os


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


def write_glossary_to_new_file(
    glossary_df: pd.DataFrame,
    dest_path: str,
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
    
def enrich_glossary_with_metadata(
    glossary_df: pd.DataFrame,
    src_workbook: Path,
    glossary_sheet_name: str = "Glossary",
    var_col: str = "Variable_name",
    code_col: str = "Code_Query",
    desc_col: str = "Description",
) -> pd.DataFrame:
    """
    Append two rows (Code_Query & Description) for every variable that appears in
    ``glossary_df['Row4']`` by looking them up in the *Glossary* sheet of the
    original workbook.

    Parameters
    ----------
    glossary_df : pd.DataFrame
        The dataframe that already contains the three columns
        ``['Row3', 'Row4', 'Sheet']`` created by ``build_glossary_dataframe``.
    src_workbook : pathlib.Path
        Path to the original workbook that holds the reference “Glossary” sheet.
    glossary_sheet_name : str, default "Glossary"
        Name of the sheet that stores the master list of variables.
    var_col : str, default "Variable_name"
        Column in the reference sheet that holds the variable identifiers.
    code_col : str, default "Code_Query"
        Column that holds the *code query* we want to pull.
    desc_col : str, default "Description"
        Column that holds the description we want to pull.

    Returns
    -------
    pd.DataFrame
        ``glossary_df`` with two additional rows for each variable (code query &
        description).  If a variable cannot be found, the added rows contain
        empty strings.
    """
    # ------------------------------------------------------------------
    # 1️⃣  Load the reference sheet (all values as strings – keep blanks).
    # ------------------------------------------------------------------
    ref_df = pd.read_excel(
        src_workbook,
        sheet_name=glossary_sheet_name,
        dtype=str,          # forces NaN → None/np.nan later, we will replace
    )

    # Normalise column names – strip spaces and enforce exact spelling
    ref_df = ref_df.rename(columns=lambda x: x.strip())

    # Make a quick lookup dictionary: variable → (code, description)
    # We use a case‑insensitive key to be forgiving.
    lookup = {}
    for _, row in ref_df.iterrows():
        var = str(row.get(var_col, "")).strip()
        if not var:
            continue
        lookup[var.lower()] = (
            str(row.get(code_col, "")).strip(),
            str(row.get(desc_col, "")).strip(),
        )

    # ------------------------------------------------------------------
    # 2️⃣  Walk through every variable that appears in the *second* column
    #     of the glossary we already built (i.e. ``Row4``).
    # ------------------------------------------------------------------
    rows_to_append = []
    for var in glossary_df["Row4"].dropna().unique():
        key = str(var).strip().lower()
        code, desc = lookup.get(key, ("", ""))   # default → empty strings

        # First extra row → Code_Query
        rows_to_append.append(
            {"Row3": var, "Row4": code, "Sheet": ""}   # keep order identical to original df
        )
        # Second extra row → Description
        rows_to_append.append(
            {"Row3": var, "Row4": desc, "Sheet": ""}
        )

    # ------------------------------------------------------------------
    # 3️⃣  Concatenate the new rows to the original dataframe.
    # ------------------------------------------------------------------
    if rows_to_append:
        extra_df = pd.DataFrame(rows_to_append, columns=glossary_df.columns)
        enriched = pd.concat([glossary_df, extra_df], ignore_index=True)
    else:
        enriched = glossary_df.copy()

    return enriched


if __name__ == "__main__":
    
    #provide the name of an input file that is located in the same folder as the script
    input_file = "ICASA_for_agroforstry_draft_4.xlsx"
    #provide a name of the output file
    output_file = "glossary"
    
    BASE_DIR = os.path.abspath(os.path.dirname(__file__)) #do not run this line alone, only works when entire scrip is run
    input_path = os.path.join(BASE_DIR, input_file)
    output_path = os.path.join(BASE_DIR, output_file)
    
    glossary = build_glossary_dataframe(input_path, (2,3))
    write_glossary_to_new_file(glossary, output_path)
