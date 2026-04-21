from pathlib import Path
import ast
import csv
import duckdb
from pandas import DataFrame, NA, notna
import numpy as np
import re

KEYWORDS = [t[0] for t in duckdb.sql("select * from duckdb_keywords()").fetchall()]
REQ_TYPES = ["optional", "recommended", "required", "hidden"]
DEFAULT_RECORD_GROUPS = ("My record types", "People and organisations", "Place, features")

"""
Here is a first version, ready for use, for a Heurist schema reader
"""

def concat_attributes(df: DataFrame, out_col: str = "attributes", sep: str = ", ") -> DataFrame:
    to_keep = []
    for col in df.columns:
        if "H-ID" in col:
            to_keep.append(col)
    out_df = df[to_keep]
    df = df.drop(columns=to_keep)

    def join_row(row) -> str:
        vals = [
            str(v).strip()
            for v in row
            if notna(v) and str(v).strip() not in ("", "None", "nan", "<NA>")
        ]
        return sep.join(vals)

    out_df[out_col] = df.apply(join_row, axis=1)
    return out_df


def normalize_heurist_date(d):
    if not isinstance(d, dict):
        return None
    if d.get("value") is not None:
        return f"{d['value']["year"]}"
    min_date = d.get("estMinDate")
    max_date = d.get("estMaxDate")
    if min_date and max_date:
        return f"{min_date["year"]} - {max_date["year"]}"
    elif min_date:
        return f"after {min_date["year"]}"
    elif max_date:
        return f"before {max_date["year"]}"
    return None


def parse_date_interval(d):
    """
    Converts a raw (dict) or normalized (str) Heurist date
    into the interval [start, end].
    """
    if isinstance(d, dict):
        if d.get("value") is not None:
            year = d["value"]["year"] if "year" in d["value"].keys() else int(str(d["value"]).split("-", 1)[0])
            return year, year
        min_date = d.get("estMinDate")
        max_date = d.get("estMaxDate")
        start = min_date["year"] if isinstance(min_date, dict) else None
        end = max_date["year"] if isinstance(max_date, dict) else None
        return start, end

    if isinstance(d, str):
        d = d.strip()
        # "1234"
        if re.fullmatch(r"-?\d{1,4}", d):
            year = int(d)
            return year, year
        # "1234 - 1250"
        m = re.fullmatch(r"(-?\d{1,4})\s*-\s*(-?\d{1,4})", d)
        if m:
            return int(m.group(1)), int(m.group(2))
        # "after 1234"
        m = re.fullmatch(r"after\s+(-?\d{1,4})", d)
        if m:
            return int(m.group(1)), None
        # "before 1250"
        m = re.fullmatch(r"before\s+(-?\d{1,4})", d)
        if m:
            return None, int(m.group(1))

    return None, None


def empty_lists_to_na(df: DataFrame) -> DataFrame:
    """
    A function to change empty lists to NaN
    """
    def fix_cell(x):
        return NA if isinstance(x, np.ndarray) and x.size == 0 else x
    return df.map(fix_cell)


def too_empty_columns(df: DataFrame, to_keep_anyway: list = None,
                      threshold: float = 0.05, drop: bool = True) -> DataFrame:
    """
    A function to consider too empty columns from a DataFrame output
    """
    df = empty_lists_to_na(df)
    non_null_frac = df.notna().mean()
    if drop:
        dropped_cols = non_null_frac[non_null_frac < threshold].index.tolist()
        kept_cols = non_null_frac[non_null_frac >= threshold].index.tolist()
        if to_keep_anyway:
            for col in to_keep_anyway:
                if col in dropped_cols:
                    dropped_cols.remove(col)
                    kept_cols.append(col)
        print(f"Dropping {len(dropped_cols)} columns with < {int(threshold * 100)}% filled values:")
        for c in dropped_cols:
            print(f"  - {c} ({non_null_frac[c] * 100:.2f}%)")
        return df[kept_cols]
    else:
        return DataFrame(non_null_frac)


def def_requirements(
        path: Path | str,
        req_types: list[str] | str = None
) -> dict:
    """
    Organize the requirement level of each column from the schema of tables
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