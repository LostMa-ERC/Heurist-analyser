import subprocess
import duckdb
import pandas as pd
from pathlib import Path
from .general import def_requirements
from .lostma_tables import LOSTMA_TABLES


class LostmaDB:
    def __init__(self, login, password, duckdb_path: str | Path | None = None):
        self.database = "jbcamps_gestes"
        self.login = login
        self.password = password
        self.cli_path = "heurist"
        base = Path.cwd()
        self.duckdb_path = Path(duckdb_path) if duckdb_path else base / "lostma.db"
        self.schema_dir = Path(self.database + "_schema")
        self._con = None
        self._requirements = None

    def download_database(self, type_arg: list = None) -> None:
        """
        Use Heurist-API CLI to download the db
            heurist -d DB -l LOGIN -p PASSWORD download -f FILE.DB
        """
        cmd = [
            self.cli_path,
            "-d", self.database,
            "-l", self.login,
            "-p", self.password,
            "download",
            "-f", str(self.duckdb_path),
              ] + type_arg
        subprocess.run(cmd, check=True)

    def download_schema(self, type_arg: list = None) -> None:
        """
        Use Heurist-API CLI to download the schema
            heurist -d DB -l LOGIN -p PASSWORD schema -t csv
        """
        cmd = [
            self.cli_path,
            "-d", self.database,
            "-l", self.login,
            "-p", self.password,
            "schema",
            "-t", "csv",
        ] + type_arg
        subprocess.run(cmd, check=True)

    def _close_connection(self):
        if self._con is not None:
            self._con.close()
            self._con = None

    def _get_requirements(self, name_table: str):
        if self._requirements is None:
            self._requirements = def_requirements(self.schema_dir)
        return self._requirements.get(name_table, {})

    def _get_columns(self, sql_name: str) -> list[str]:
        rows = self.sql(
            "SELECT column_name FROM information_schema.columns WHERE table_name = ?;",
            [sql_name],
            is_df=False,
        ).fetchall()
        return [r[0] for r in rows]

    def sync(self, type_table: str = None) -> None:
        """
        Download the db and its schema
        """
        if type_table:
            type_arg = ["-r", type_table]
        else:
            type_arg = []
        self._close_connection()
        self.download_database(type_arg)
        self.download_schema(type_arg)

    def sql(self, query: str, params: list = None, is_df : bool = True):
        """
        Execute a request and return a dataframe
        """
        if self._con is None:
            self._con = duckdb.connect(self.duckdb_path)
        res = self._con.execute(query, params)
        if is_df:
            res = res.fetchdf()
        return res

    def table(self, base_table: str, condition: str = None , joins: list[dict] = None):
        """
        Return the content of a table
            Filter on a condition and add joins if there are any
        """
        if joins:
            # build a specific select to avoid ambiguous column names on joined tables
            join_tables = [j["table"] for j in (joins or [])]
            all_tables = [base_table] + join_tables
            table_cols: dict[str, list[str]] = {}
            for t in all_tables:
                table_cols[t] = self._get_columns(t)
            col_count: dict[str, int] = {}
            for cols in table_cols.values():
                for c in cols:
                    col_count[c] = col_count.get(c, 0) + 1
            select_expr = []
            for t in all_tables:
                for c in table_cols[t]:
                    col_ref = f'{t}."{c}"'
                    if t != base_table and col_count[c] > 1:
                        alias = f'{c}_{t}'
                        select_expr.append(f'{col_ref} AS "{alias}"')
                    else:
                        select_expr.append(col_ref)
            select_clause = ",\n    ".join(select_expr)
            query = f"SELECT\n    {select_clause}\nFROM {base_table} "
            for join in joins:
                query += " ".join(join.values())
        else:
            query = f"SELECT * FROM {base_table} "
        if condition:
            query += condition
        return self.sql(query)

    def texts(self, languages: list | str = None):
        """
        Return the content of the text table
            Filter on the language_COLUMN attribute (ex: 'dum (Middle Dutch)')
        """
        if languages:
            if isinstance(languages, str):
                languages = [languages]
            condition = f"WHERE language_COLUMN IN ('{"', '".join(languages)}')"
            return self.table("TextTable", condition)
        return self.table("TextTable")

    def witnesses(self, languages: list | str = None):
        """
        Return the content of the witness table
            Filter on the language_COLUMN text attribute (ex: 'dum (Middle Dutch)')
        """
        condition = ""
        joins = [{"type_join": "LEFT JOIN", "table": "TextTable",
                  "on": "ON witness.\"is_manifestation_of H-ID\" = TextTable.\"H-ID\" "},
                 {"type_join": "LEFT JOIN", "table":  "Part",
                  "on":  "ON list_contains(witness.\"observed_on_pages H-ID\", part.\"H-ID\") = TRUE "},
                 {"type_join": "LEFT JOIN", "table":  "DocumentTable",
                  "on":  "ON part.\"is_inscribed_on H-ID\" = DocumentTable.\"H-ID\" "}]
        if languages:
            if isinstance(languages, str):
                languages = [languages]
            condition = f"WHERE TextTable.language_COLUMN IN ('{"', '".join(languages)}')"
        return self.table("Witness", condition, joins)

    def is_table_exists(self, table_name: str, sql_name: str) -> None:
        """Check if table is available on the db, if not download it"""
        row = self.sql(
            "SELECT 1 FROM duckdb_tables WHERE table_name = ?;",
            [sql_name],
            is_df=False
        ).fetchone()
        if not row:
            type_table = LOSTMA_TABLES[table_name]["type"]
            print(f"Table {table_name} is not available. Downloading...")
            self.sync(type_table)

    def analyse(self, name_table: str = None,
                language: str = None) -> dict | str:
        """
        A function to analyse the completeness of each table for each corpus
        """
        if name_table[0].isupper():
            name_table = name_table[0].lower() + name_table[1:]
        sql_name = LOSTMA_TABLES[name_table]["safe_sql_name"]
        self.is_table_exists(name_table, sql_name)
        rows = self.sql(
            "SELECT column_name, data_type "
            "FROM information_schema.columns "
            "WHERE table_name = ?;",
            [sql_name],
            is_df=False,
        ).fetchall()
        col_types = {name: dtype for (name, dtype) in rows}
        columns = list(col_types.keys())
        requirements = self._get_requirements(sql_name)
        action_required = "No field for this table"
        if LOSTMA_TABLES[name_table]["is_corpus_data"]:
            len_table = self.sql(LOSTMA_TABLES[name_table]["len_query"], [language], is_df=False).fetchone()[0]
            if len_table and LOSTMA_TABLES[name_table]["action_required"]:
                action_required = self.sql(LOSTMA_TABLES[name_table]["action_required"], [language],
                                           is_df=False).fetchone()[0]
        else:
            len_table = self.sql(LOSTMA_TABLES["non-corpus tables"]["len_query"].format(table=sql_name),
                                 is_df=False).fetchone()[0]
            if len_table and LOSTMA_TABLES[name_table]["is_action_required"]:
                action_required = self.sql(LOSTMA_TABLES["non-corpus tables"]["action_required"].format(table=sql_name),
                                           is_df=False).fetchone()[0]
        if len_table:
            agg_expr = []
            col_metadata = []
            for column in columns:
                if column in ["H-ID", "type_id"] or "TRM-ID" in column:
                    continue
                req_type = requirements.get(column)
                if req_type is None:
                    continue
                dtype = col_types[column]
                if dtype.endswith('[]'):
                    expr = f"""
                    COUNT(*) FILTER (
                      WHERE "{sql_name}"."{column}" IS NULL
                         OR array_length("{sql_name}"."{column}") = 0
                    ) AS "{column}"
                    """
                else:
                    expr = f"""
                    COUNT(*) FILTER (WHERE "{sql_name}"."{column}" IS NULL) AS "{column}"
                    """
                agg_expr.append(expr)
                col_metadata.append((column, req_type))
            agg_sql = ",\n".join(agg_expr)
            if LOSTMA_TABLES[name_table]["is_corpus_data"]:
                base_clause = LOSTMA_TABLES[name_table]["detail_query"].format(table=sql_name)
                query = f"SELECT {agg_sql} {base_clause};"
                params = [language]
            else:
                query = f"SELECT {agg_sql} FROM {sql_name};"
                params = []
            row = self.sql(query, params, is_df=False).fetchone()
            list_empty = []
            for (i, (column, req_type)) in enumerate(col_metadata):
                count_empty = row[i]
                list_empty.append({
                    "field": column,
                    "required statement": req_type,
                    "empty records": count_empty,
                    "percentage empty": round((count_empty / len_table) * 100, 2),
                })
            return {
                "completeness table": pd.DataFrame(list_empty),
                "total records": len_table,
                "action required": action_required
            }
        else:
            return "No data"

    def tradition(self, languages: list = None):
        """
            Return the data necessary to study the tradition of manuscripts
        """
        query = ("SELECT witness.\"H-ID\" AS witness_id, TextTable.\"H-ID\" AS text_id FROM witness "
                 "INNER JOIN TextTable ON witness.\"is_manifestation_of H-ID\" = TextTable.\"H-ID\" ")
        if languages:
            if isinstance(languages, str):
                languages = [languages]
            query += f"WHERE TextTable.language_COLUMN IN ('{"', '".join(languages)}')"
        return self.sql(query)


def interval(table: pd.DataFrame, attribute: str, year_min: int, year_max: int) -> pd.DataFrame:
    """
    A filter that extracts data from a specific time interval
    """
    if isinstance(year_min, str):
        year_min = str(year_min)
    if isinstance(year_max, str):
        year_max = str(year_max)

    def extract_interval(d):
        if not isinstance(d, dict):
            return pd.NaT, pd.NaT
        if "value" in d and d["value"]:
            return d["value"], d["value"]
        start = d.get("estMinDate")
        end = d.get("estMaxDate")
        if not start and not end:
            return pd.NaT, pd.NaT
        return start, end

    intervals = table[attribute].apply(extract_interval)
    intervals = pd.DataFrame(intervals.tolist(), index=table.index, columns=["start", "end"])
    intervals["start"] = (
        intervals["start"].astype(str)
        .str.split("-", n=1).str[0]
    )
    intervals["start"] = pd.to_numeric(intervals["start"], errors="coerce")
    intervals["end"] = (
        intervals["end"].astype(str)
        .str.split("-", n=1).str[0]
    )
    intervals["end"] = pd.to_numeric(intervals["end"], errors="coerce")
    mask = (intervals["end"] >= year_min) & (intervals["start"] <= year_max)
    return table[mask]