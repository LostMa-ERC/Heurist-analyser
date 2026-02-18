import duckdb
import pandas as pd
from pathlib import Path
from .general import def_requirements
from .lostma_tables import LOSTMA_TABLES
from .tei_depot import TeiDepotClient
from heurist.api.connection import HeuristAPIConnection
from heurist.workflows.etl import extract_transform_load
from heurist.schema import export_schema
from heurist.utils.constants import DEFAULT_RECORD_GROUPS


class LostmaDB:
    def __init__(self, login, password, duckdb_path: str | Path | None = None):
        self.database = "jbcamps_gestes"
        self.login = login
        self.password = password
        base = Path.cwd()
        self.duckdb_path = Path(duckdb_path) if duckdb_path else base / "lostma.db"
        self.schema_dir = Path(self.database + "_schema")
        self._con = None
        self._requirements = None

    def download_database(self, type_arg: tuple = DEFAULT_RECORD_GROUPS) -> None:
        """
        Use Heurist-API to download the DB
        """
        with HeuristAPIConnection(self.database, self.login, self.password) as client:
            conn = duckdb.connect(self.duckdb_path)
            extract_transform_load(
                client=client, duckdb_connection=conn, record_group_names=type_arg
            )

    def download_schema(self, type_arg: tuple = DEFAULT_RECORD_GROUPS) -> None:
        """
        Use Heurist-API to download the schema
        """
        export_schema(
            db_name=self.database,
            login=self.login,
            password=self.password,
            debugging=False,
            output_type="csv",
            record_group=type_arg,
            outdir=self.database + "_schema"
        )

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

    def _is_table_exists(self, table_name: str, sql_name: str = None) -> None:
        """Check if table is available on the db, if not download it"""
        if not sql_name:
            sql_name = LOSTMA_TABLES[table_name]["safe_sql_name"]
        row = self.sql(
            "SELECT 1 FROM duckdb_tables WHERE table_name = ?;",
            [sql_name],
            is_df=False
        ).fetchone()
        if not row:
            type_table = LOSTMA_TABLES[table_name]["type"]
            print(f"Table {table_name} is not yet available. Downloading...")
            self.sync(type_table)

    def sync(self, type_table: str = None) -> None:
        """
        Download the db and its schema
        """
        if type_table:
            type_table = tuple(type_table)
        else:
            type_table = DEFAULT_RECORD_GROUPS
        self._close_connection()
        self.download_database(type_table)
        self.download_schema(type_table)

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

    def table(self,
              base_table: str,
              condition: str = None ,
              joins: list[dict] = None,
              selects: dict[str, dict] = None):
        """
        Return the content of a table
            Filter on a condition and add joins if there are any
        """

        def build_selects(cols_by_table):
            """Build the select part of the query from a dictionary
            of attributes ordered by table"""
            select_expr = []
            for table in cols_by_table:
                for att in cols_by_table[table]:
                    a = f"{table}.{att} AS \"{table}_{att.replace("\"", "")}\""
                    select_expr.append(a)
            select_clause = ",\n    ".join(select_expr)
            select_query = f"SELECT\n    {select_clause}\nFROM {base_table} "
            return select_query

        if selects:
            table_cols: dict[str, list[str]] = {}
            recursives = []
            for t in selects:
                table_cols[t] = selects[t]["attributes"]
                if "recursive" in selects[t].keys():
                    for recursive in selects[t]["recursive"]:
                        walk = t + "_walk"
                        recursive_query = f""" {walk} AS (
                        SELECT
                              c."H-ID"          AS child_id,
                              c."{recursive}"   AS parent_id,
                              1                 AS depth,
                              [c."H-ID"]        AS path
                        FROM {t} c
                        
                        UNION ALL
                        
                        SELECT
                            {walk}.child_id,
                            p."{recursive}"             AS parent_id,
                            {walk}.depth + 1            AS depth,
                            {walk}.path || [p."H-ID"]   AS path
                        FROM {walk}
                        JOIN {t} p
                        ON p."H-ID" = {walk}.parent_id
                        WHERE {walk}.parent_id IS NOT NULL
                        AND NOT list_contains({walk}.path, {walk}.parent_id)
                        ),
                        {t}_ancestors AS (
                            SELECT
                                {walk}.child_id,
                                {walk}.depth,
                                p.preferred_name AS ancestor_name
                            FROM {walk}
                            JOIN {t} p ON p."H-ID" = {walk}.parent_id
                        ),
                        {t}_titles AS (
                            SELECT
                                child_id,
                                string_agg(ancestor_name, ' > ' ORDER BY depth) AS {t}_ancestor_titles
                            FROM {t}_ancestors
                            GROUP BY child_id
                        )
                        """
                        recursives.append(recursive_query)
                        table_cols[t].append(f"{t}_ancestor_titles")
                        joins.append(
                            {"type_join": "LEFT JOIN", "table": f"{t}_titles",
                             "on": f"ON {t}_titles.\"{recursive}\" = {t}.\"H-ID\" "}
                        )
            query = build_selects(table_cols)
            if recursives:
                start_recursive = "WITH RECURSIVE"
                query = start_recursive + "\n    " + ",\n    ".join(recursives) + "\n    " + query
        else:
            query = "SELECT * "
        if joins:
            join_tables = [j["table"] for j in (joins or [])]
            for join_table in join_tables:
                if "_titles" not in join_table:
                    name_table = join_table.split(" ")[0]
                    normal_name = LOSTMA_TABLES[name_table]["normal_name"]
                    self._is_table_exists(normal_name, join_table)
            if not selects:
                all_tables = [base_table] + join_tables
                table_cols: dict[str, list[str]] = {}
                for t in all_tables:
                    table_cols[t] = self._get_columns(t)
                query = build_selects(table_cols)
            for join in joins:
                query += " ".join(join.values())
        else:
            query += f"FROM {base_table} "
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
        select = {"Witness": {"attributes": ["\"H-ID\"", "is_unobserved", "claim_freetext", "preferred_siglum",
                                             "alternative_sigla", "status_witness", "status_notes", "is_excerpt",
                                             "scripta_freetext", "date_of_creation", "date_of_creation_certainty",
                                             "date_of_creation_source", "date_freetext", "number_of_hands",
                                             "scribe_note", "place_of_creation_source"]},
                  "TextTable": {"attributes": ["\"H-ID\"", "preferred_name", "language", "literary_form",
                                               "is_hypothetical", "claim_freetext", "length", "length_freetext",
                                               "verse_type", "rhyme_type", "stanza_type", "nature_of_derivation",
                                               "tradition_status", "status_notes", "scripta_freetext",
                                               "date_of_creation", "date_of_creation_certainty",
                                               "date_of_creation_source", "date_freetext", "author_freetext",
                                               "place_of_creation_source", "is_derived_from",
                                               "\"observed_on_pages H-ID\""]},
                  "Witness_last_observed_in_doc": {"attributes": ["\"H-ID\"", "collection", "current_shelfmark",
                                                                  "invented_label"]},
                  "Text_is_derived_from": {"attributes": ["\"H-ID\"", "preferred_name"]},
                  "Stemma": {"attributes": ["\"H-ID\"", "\"openstemmata-id\""]},
                  "Story": {"attributes": ["\"H-ID\"", "preferred_name", "peripheral"]},
                  "Genre": {"attributes": ["\"H-ID\"", "preferred_name"],
                            "recursive": ["parent_genre H-ID"]},
                  "Storyverse": {"attributes": ["\"H-ID\"", "preferred_name"],
                                 "recursive": ["member_of_cycle H-ID"]},
                  "Witness_regional_writing_style": {"attributes": ["\"H-ID\"", "preferred_name", "language"]},
                  "Text_regional_writing_style": {"attributes": ["\"H-ID\"", "preferred_name", "language"]},
                  "Witness_scribe": {"attributes": ["\"H-ID\"", "given_names", "family_name", "floruit",
                                                    "date_of_birth", "date_of_death"]},
                  "Text_author": {"attributes": ["\"H-ID\"", "given_names", "family_name", "floruit", "date_of_birth",
                                                 "date_of_death"]},
                  "Text_adaptator": {"attributes": ["\"H-ID\"", "given_names", "family_name", "floruit",
                                                    "date_of_birth", "date_of_death"]},
                  "Witness_place_of_creation": {"attributes": ["\"H-ID\"", "place_name", "administrative_region",
                                                               "country"]},
                  "Text_place_of_creation": {"attributes": ["\"H-ID\"", "place_name", "administrative_region",
                                                            "country"]},
                  "Witness_last_observed_in_doc_location": {"attributes": ["\"H-ID\"", "place_name",
                                                                           "administrative_region", "country"]}}
        condition = ""
        joins = [{"type_join": "LEFT JOIN", "table": "Scripta Witness_regional_writing_style",
                  "on": "ON Witness.\"regional_writing_style H-ID\" = Witness_regional_writing_style.\"H-ID\" "},
                 {"type_join": "LEFT JOIN", "table": "Person Witness_scribe",
                  "on": "ON Witness.\"scribe H-ID\" = Witness_scribe.\"H-ID\" "},
                 {"type_join": "LEFT JOIN", "table": "DocumentTable Witness_last_observed_in_doc",
                  "on": "ON Witness.\"last_observed_in_doc H-ID\" = Witness_last_observed_in_doc.\"H-ID\" "},
                 {"type_join": "LEFT JOIN", "table": "Place Witness_last_observed_in_doc_location",
                  "on": "ON Witness_last_observed_in_doc.\"location H-ID\" "
                        "= Witness_last_observed_in_doc_location.\"H-ID\" "},
                 {"type_join": "LEFT JOIN", "table": "Place Witness_place_of_creation",
                  "on": "ON Witness.\"place_of_creation H-ID\" = Witness_place_of_creation.\"H-ID\" "},
                 {"type_join": "LEFT JOIN", "table": "TextTable",
                  "on": "ON Witness.\"is_manifestation_of H-ID\" = TextTable.\"H-ID\" "},
                 {"type_join": "LEFT JOIN", "table": "Genre",
                  "on": "ON TextTable.\"specific_genre H-ID\" = Genre.\"H-ID\" "},
                 {"type_join": "LEFT JOIN", "table": "Story",
                  "on": "ON TextTable.\"is_expression_of H-ID\" = Story.\"H-ID\" "},
                 {"type_join": "LEFT JOIN", "table": "Storyverse",
                  "on": "ON Story.\"is_part_of_storyverse H-ID\" = Storyverse.\"H-ID\" "},
                 {"type_join": "LEFT JOIN", "table": "Scripta Text_regional_writing_style",
                  "on": "ON TextTable.\"regional_writing_style H-ID\" = Text_regional_writing_style.\"H-ID\" "},
                 {"type_join": "LEFT JOIN", "table": "Person Text_author",
                  "on": "ON TextTable.\"is_written_by H-ID\" = Text_author.\"H-ID\" "},
                 {"type_join": "LEFT JOIN", "table": "Person Text_adaptator",
                  "on": "ON TextTable.\"is_adapted_by H-ID\" = Text_adaptator.\"H-ID\" "},
                 {"type_join": "LEFT JOIN", "table": "Place Text_place_of_creation",
                  "on": "ON TextTable.\"place_of_creation H-ID\" = Text_place_of_creation.\"H-ID\" "},
                 {"type_join": "LEFT JOIN", "table": "Stemma",
                  "on": "ON TextTable.\"in_stemma H-ID\" = Stemma.\"H-ID\" "},
                 {"type_join": "LEFT JOIN", "table": "TextTable Text_is_derived_from",
                  "on": "ON TextTable.\"is_derived_from H-ID\" = Text_is_derived_from.\"H-ID\" "}
                 ]
        if languages:
            if isinstance(languages, str):
                languages = [languages]
            condition = f"WHERE TextTable.language_COLUMN IN ('{"', '".join(languages)}')"
        return self.table("Witness", condition, joins, select)

    def analyse(self, name_table: str = None,
                language: str = None) -> dict | str:
        """
        A function to analyse the completeness of each table for each corpus
        """
        if name_table[0].isupper():
            name_table = name_table[0].lower() + name_table[1:]
        sql_name = LOSTMA_TABLES[name_table]["safe_sql_name"]
        self._is_table_exists(name_table, sql_name)
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


def filter_by_interval(table: pd.DataFrame, attribute: str, year_min: int, year_max: int) -> pd.DataFrame:
    """
    A filter that extracts data from a specific time interval
    """
    if isinstance(year_min, str):
        year_min = int(year_min)
    if isinstance(year_max, str):
        year_max = int(year_max)

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


def temporal_extent(table: pd.DataFrame, attribute: str) -> tuple[int | None, int | None]:
    mins, maxs = [], []
    for v in table[attribute]:
        if not isinstance(v, dict):
            continue
        if "value" in v and v["value"]:
            mins.append(v["value"].year)
            maxs.append(v["value"].year)
        else:
            mins.append(v.get("estMinDate").year)
            maxs.append(v.get("estMaxDate").year)
    date_min = min(mins)
    date_max = max(maxs)
    return date_min, date_max


def download_text_in_tei(text_ids: list[int] | int | str):
    """
    Download the TEI file corresponding to the id from their GitHub repository
    """
    if not isinstance(text_ids, list):
        text_ids = [text_ids]
    client = TeiDepotClient()
    for text_id in text_ids:
        text_id = str(text_id)
        file = client.download_by_id(text_id, dest_dir="output")
        print("Downloaded :", file)