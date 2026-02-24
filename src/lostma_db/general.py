from pathlib import Path
import ast
import csv
import duckdb
from pandas import DataFrame, NA

KEYWORDS = [t[0] for t in duckdb.sql("select * from duckdb_keywords()").fetchall()]
REQ_TYPES = ["optional", "recommended", "required", "hidden"]

"""
Here is a first version, ready for use, for a Heurist schema reader
"""


def normalize_heurist_date(d):
    if not isinstance(d, dict):
        return None
    if d.get("value") is not None:
        return f"{d["value"].year}"
    min_date = d.get("estMinDate")
    max_date = d.get("estMaxDate")
    if min_date and max_date:
        return f"{min_date.year} - {max_date.year}"
    elif min_date:
        return f"after {min_date.year}"
    elif max_date:
        return f"before {max_date.year}"
    return None


def empty_lists_to_na(df: DataFrame) -> DataFrame:
    """
    A function to change empty lists to NaN
    """
    def fix_cell(x):
        return NA if isinstance(x, list) and len(x) == 0 else x
    return df.applymap(fix_cell)


def drop_too_empty_columns(df: DataFrame, threshold: float = 0.10) -> DataFrame:
    """
    A function to drop too empty columns from a dataframe
    """
    df = empty_lists_to_na(df)
    non_null_frac = df.notna().mean()
    dropped_cols = non_null_frac[non_null_frac < threshold].index.tolist()
    kept_cols = non_null_frac[non_null_frac >= threshold].index.tolist()
    print(f"Dropping {len(dropped_cols)} columns with < {int(threshold * 100)}% filled values:")
    for c in dropped_cols:
        print(f"  - {c} ({non_null_frac[c] * 100:.2f}%)")
    return df[kept_cols]


def def_requirements(
        path: Path | str,
        req_types: list[str] | str = None
) -> dict:
    """
    Organise the requirement level of each column from the schema of tables
    """
    if req_types is None:
        req_types = REQ_TYPES
    data = {}
    if not isinstance(req_types, list):
        req_types = [req_types]
    for file in path.iterdir():
        file_name = file.name.split(".")[0]
        data[file_name] = {}
        with open(file, 'r') as csv_file:
            reader = csv.DictReader(csv_file, delimiter=',')
            for row in reader:
                # Il faut nettoyer 2-3 trucs issus de la table Stemma...
                # En fait il faudrait que le schéma soit rédigé avec les mêmes transfo que les tables de la DuckDB
                name_detail = row['rst_DisplayName']
                if "-" in name_detail:
                    name_detail = name_detail.replace("-", " ")
                if name_detail == "URL(s)":
                    name_detail = "URL"
                # Si c'est une clé étrangère, il me faut ajouter un H-ID
                foreign_key = ast.literal_eval(row['dty_PtrTargetRectypeIDs'])
                if foreign_key:
                    name_detail += " H-ID"
                # Je reprends ici des trucs présent dans le fichier sql_safety de l'Heurist-API
                if name_detail.lower() in KEYWORDS:
                    name_detail = f"{name_detail}_COLUMN"
                # J'enlève les éléments du Header
                if "Header" not in row['dty_Name']:
                    for t in req_types:
                        if row['rst_RequirementType'] == t:
                            data[file_name][name_detail] = t
    return data