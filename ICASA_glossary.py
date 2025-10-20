# -*- coding: utf-8 -*-
"""
Created on Mon Oct 20 12:05:35 2025

@author: garre
"""

import pandas as pd
from pathlib import Path
import argparse
import sys


def _extract_two_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Helper: keep only rows 3 and 4 (index 2 & 3) and drop columns that are empty
    in **both** rows. Return a DataFrame of shape (n_filled_cells, 2)
    where column 0 = row‑3 value, column 1 = row‑4 value.
    """
    # Keep rows 3 and 4
    rows = df.iloc[[2, 3]]

    # Drop columns that are completely empty in the two rows
    rows = rows.dropna(axis=1, how="all")

    # If nothing left, return an empty DataFrame (caller will skip it)
    if rows.empty:
        return pd.DataFrame()

    # Transpose: each original column becomes a row → (n_cells, 2)
    transposed = rows.T.reset_index(drop=True)
    transposed.columns = ["Row3", "Row4"]          # rename the two columns
    return transposed


def build_glossary_dataframe(
    src_path: Path,
    row_indices: tuple[int, int] = (2, 3)   # 0‑based for Excel rows 3 & 4
) -> pd.DataFrame:
    """
    Reads *src_path* and returns the stacked glossary DataFrame.
    """
    xl = pd.ExcelFile(src_path, engine="openpyxl")
    all_blocks = []                         # list of per‑sheet DataFrames

    for sheet_name in xl.sheet_names:
        # Load only the first 4 rows – everything else is unnecessary.
        # dtype=str forces everything to be read as text (keeps numbers,
        # dates, etc. as their string representation – you can drop it if you
        # prefer native dtypes).
        raw = xl.parse(
            sheet_name=sheet_name,
            header=None,          # no header rows in the source sheets=str,
        )

        # Extract the two rows, transpose, and give them column names.
        block = _extract_two_rows(raw)

        if block.empty:
            # No data in rows 3‑4 for this sheet → just skip it.
            print(f"[INFO] Sheet '{sheet_name}' has no data in rows 3‑4 – skipped.")
            continue

        # Add the sheet name as a third column
        block["Sheet"] = sheet_name

        # Re‑order columns to match the required output order
        block = block[["Row3", "Row4", "Sheet"]]

        all_blocks.append(block)

    if not all_blocks:
        raise ValueError("No sheet contained data in rows 3‑4.")

    # Stack everything vertically (one sheet after another)
    glossary_df = pd.concat(all_blocks, axis=0, ignore_index=True)
    return glossary_df


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


# ----------------------------------------------------------------------
#  Command‑line interface – useful for quick testing
# ----------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Build a two‑column glossary from rows 3‑4 of every sheet."
    )
    parser.add_argument(
        "source_file",
        type=Path,
        help="Path to the existing workbook (must be .xlsx or .xlsm).",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("glossary.xlsx"),
        help="Destination file name (default: glossary.xlsx).",
    )
    parser.add_argument(
        "--sheet-name",
        default="glossary",
        help="Name of the sheet that will hold the glossary (default: glossary).",
    )
    args = parser.parse_args()

    # Basic sanity checks
    if not args.source_file.is_file():
        sys.exit(f"❌ Source file '{args.source_file}' does not exist.")
    if args.source_file.suffix.lower() not in {".xlsx", ".xlsm"}:
        sys.exit("❌ Only .xlsx or .xlsm workbooks are supported (openpyxl engine).")

    # Build the DataFrame
    glossary = build_glossary_dataframe(args.source_file)

    # Write it out
    write_glossary_to_new_file(glossary, args.out, sheet_name=args.sheet_name)