import duckdb
import pandas as pd
from pathlib import Path

from duckdb.experimental.spark import DataFrame

from .general import (def_requirements, normalize_heurist_date,
                      too_empty_columns, concat_attributes,
                      DEFAULT_RECORD_GROUPS, empty_lists_to_na)
from .lostma_tables import LOSTMA_TABLES
from .tei_depot import TeiDepotClient
from heurist.api.connection import HeuristAPIConnection
from heurist.workflows.etl import extract_transform_load
from heurist.schema import export_schema
from numpy import ndarray

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
            is_df=False
        ).fetchall()
        return [r[0] for r in rows]

    def _is_list(self, table: str, attribute: str) -> bool:
        attribute_type = self.sql(
            """SELECT data_type
               FROM information_schema.columns
               WHERE table_name = ?
               AND column_name = ?""",
            [table, attribute],
            is_df=False
        ).fetchone()[0]
        if attribute_type.endswith("[]") or attribute_type.startswith("LIST"):
            return True
        else:
            return False

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

    def _solving_tables(self, data: DataFrame, references: dict) -> DataFrame:
        """
        Organize the research of each reference in the corresponding tables
        """
        for table in references:
            if "depends_on" in references[table].keys():
                solving = self._solving_references(data, table, references[table], only_search=True)
                depends_on = references[table]["depends_on"]
                for dependency in depends_on:
                    solving = self._solving_references(solving, dependency, depends_on[dependency], keep_fks=True)
                    solving = concat_attributes(solving, "attributes")
                data = self._solving_references(data, table, references[table], only_solve=True, solving=solving)
            else:
                data = self._solving_references(data, table, references[table])
        return data

    def _solving_references(self, to_complete: DataFrame, name_table: str, complement: dict,
                            only_search: bool = False, only_solve: bool = False,
                            solving: DataFrame = None, keep_fks: bool = False) -> DataFrame:
        """
        Resolve an identifier field with the name of each entity
        """
        if not only_solve:
            normal_name = LOSTMA_TABLES[name_table]["normal_name"]
            self._is_table_exists(normal_name, name_table)
            solving_select = [{"name_table": name_table, "attributes": complement["attributes"]}]
            solving = self.table(name_table, selects=solving_select)
            for col in solving.columns:
                if solving[col].apply(lambda x: isinstance(x, dict)).any():
                    solving[col] = solving[col].apply(normalize_heurist_date)
            solving = concat_attributes(solving, "attributes")
            if only_search:
                return solving
        for join in complement["name_joins"]:
            name_table_attributes = join.replace("H-ID", "") + "Name"
            id_joined_table = name_table + "_H-ID"
            solving = solving.rename(columns={solving.columns[-1]: name_table_attributes})
            if keep_fks:
                fks = solving.drop(columns=[id_joined_table, name_table_attributes]).columns
                joined_fields = [name_table_attributes] + [fk for fk in fks]
            else:
                joined_fields = [name_table_attributes]
            id_to_name = solving.set_index(id_joined_table)[joined_fields].to_dict()
            table_name, attribute_name = join.split("_", 1)
            is_list = self._is_list(table_name, attribute_name)
            pos = to_complete.columns.get_loc(join) + 1
            for supp in id_to_name:

                def ids_to_names(x, multiple_value: bool):
                    if multiple_value:
                        if not isinstance(x, (list, ndarray)) or len(x) == 0:
                            return pd.NA
                        return [id_to_name[supp].get(hid) for hid in x]
                    else:
                        return id_to_name[supp].get(x)

                names_series = to_complete[join].apply(ids_to_names, multiple_value=is_list)
                to_complete.insert(pos, supp, names_series)
                pos += 1
        return to_complete

    def sync(self, type_table: str = None) -> None:
        """
        Download the db and its schema
        """
        if type_table:
            type_table = (type_table, )
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
              condition: str = None,
              joins: list[dict] = None,
              selects: list[dict] = None,
              language: str = None):
        """
        Return the content of a table
            Filter on a condition and add joins if there are any
        """

        def build_selects(ordered_columns):
            """Build the select part of the query from a dictionary
            of attributes ordered by table"""
            select_expr = []
            for table in ordered_columns:
                name = table["name_table"]
                for att in table["attributes"]:
                    a = f"{name}.{att} AS \"{name}_{att.replace("\"", "")}\""
                    select_expr.append(a)
            select_clause = ",\n    ".join(select_expr)
            select_query = f"SELECT\n    {select_clause}\nFROM {base_table} "
            return select_query

        recursives = {}
        if selects:
            for t in selects:
                name_table = t["name_table"]
                if "recursives" in t.keys():
                    new_selects = []
                    for recursive in t["recursives"]:
                        name_recursive_in_query = name_table + "_" + recursive.replace(" H-ID", "")
                        walk = name_table + "_walk"
                        if self._is_list(name_table, recursive):
                            recursive_query = f"""{walk} AS (
    SELECT
        c."H-ID"                    AS child_id,
        u.parent_id                 AS parent_id,
        [c."H-ID", u.parent_id]     AS path,
        [u.parent_id] AS lineage_ids 
    FROM {name_table} c
    CROSS JOIN UNNEST(c."{recursive}") AS u(parent_id)
    WHERE u.parent_id IS NOT NULL

    UNION ALL

    SELECT
        {walk}.child_id,
        u2.parent_id                            AS parent_id,
        {walk}.path || [u2.parent_id]           AS path,
        {walk}.lineage_ids || [u2.parent_id]    AS lineage_ids
    FROM {walk}
    JOIN {name_table} p
    ON p."H-ID" = {walk}.parent_id
    CROSS JOIN UNNEST(p."{recursive}") AS u2(parent_id)
    WHERE u2.parent_id IS NOT NULL
        AND NOT list_contains({walk}.path, u2.parent_id)
    ),
    {name_table}_leaves AS (
    SELECT {walk}.*
    FROM {walk}
    WHERE NOT EXISTS (
        SELECT 1
        FROM {name_table} p
        CROSS JOIN UNNEST(p."{recursive}") AS u2(parent_id)
        WHERE p."H-ID" = {walk}.parent_id
            AND u2.parent_id IS NOT NULL
            AND NOT list_contains({walk}.path, u2.parent_id)
        )
    ),
    {name_table}_lineage_titles AS (
    SELECT
        {name_table}_leaves.child_id,
        {name_table}_leaves.lineage_ids,
        string_agg({name_table}.preferred_name, ' > ' ORDER BY a.ord) AS lineage_title
    FROM {name_table}_leaves
    CROSS JOIN UNNEST({name_table}_leaves.lineage_ids) WITH ORDINALITY AS a(ancestor_id, ord)
    JOIN {name_table} ON {name_table}."H-ID" = a.ancestor_id
    GROUP BY {name_table}_leaves.child_id, {name_table}_leaves.lineage_ids
    ),
    {name_recursive_in_query} AS (
    SELECT
         child_id,
         list(lineage_title) AS titles
    FROM {name_table}_lineage_titles
    GROUP BY child_id
    )
    """
                        else:
                            recursive_query = f"""{walk} AS (
    SELECT
        c."H-ID"          AS child_id,
        c."{recursive}"   AS parent_id,
        1                 AS depth,
        [c."H-ID"]        AS path
    FROM {name_table} c

    UNION ALL

    SELECT
         {walk}.child_id,
         p."{recursive}"             AS parent_id,
         {walk}.depth + 1            AS depth,
         {walk}.path || [p."H-ID"]   AS path
    FROM {walk}
    JOIN {name_table} p
    ON p."H-ID" = {walk}.parent_id
    WHERE {walk}.parent_id IS NOT NULL
         AND NOT list_contains({walk}.path, {walk}.parent_id)
    ),
    {name_table}_ancestors AS (
    SELECT
        {walk}.child_id,
        {walk}.depth,
        p.preferred_name AS ancestor_name
    FROM {walk}
    JOIN {name_table} p ON p."H-ID" = {walk}.parent_id
    ),
    {name_recursive_in_query} AS (
    SELECT
         child_id,
         string_agg(ancestor_name, ' > ' ORDER BY depth) 
         AS titles
    FROM {name_table}_ancestors
    GROUP BY child_id
    )"""
                        recursives[name_recursive_in_query] = recursive_query
                        new_selects.append({"name_table": f"{name_recursive_in_query}",
                                            "attributes": [f"titles"]})
                        joins.append(
                            {"type_join": "LEFT JOIN", "table": f"{name_recursive_in_query}",
                             "on": f"ON {name_recursive_in_query}.child_id = {name_table}.\"H-ID\" "}
                        )
                    pos_table = selects.index(t)
                    for new_select in new_selects:
                        selects.insert(pos_table + 1, new_select)
            query = build_selects(selects)
            if recursives:
                start_recursive = "WITH RECURSIVE"
                query = start_recursive + "\n    " + ",\n    ".join(recursives.values()) + "\n    " + query
        else:
            query = "SELECT * "
        if joins:
            join_tables = [j["table"] for j in joins if "table" in j.keys()]
            for join_table in join_tables:
                if join_table not in recursives.keys():
                    name_table = join_table.split(" ")[0]
                    normal_name = LOSTMA_TABLES[name_table]["normal_name"]
                    self._is_table_exists(normal_name, name_table)
            if not selects:
                all_tables = [base_table] + join_tables
                table_cols: dict[int, dict] = {}
                num = 0
                for t in all_tables:
                    num += 1
                    table_cols[num] = {"name_table": t,
                                       "attributes": self._get_columns(t)}
                query = build_selects(table_cols)
            for join in joins:
                query += "\n    "
                query += " ".join(join.values())
        else:
            if not selects:
                query += f"FROM {base_table} "
        if condition:
            query += condition
        kwargs = {}
        if language:
            kwargs["params"] = [language]
        return self.sql(query, **kwargs)

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

    def witnesses(self, languages: list | str = None,
                  columns_to_keep: list = None,
                  completeness_threshold: float = None,
                  drop_empty_columns: bool = True):
        """
        Return a selection of attributes of the witness table and his linked tables
            Filter on the language_COLUMN text attribute (ex: 'dum (Middle Dutch)')
        """
        select = [{"name_table": "Witness", "attributes": ["\"H-ID\"", "\"observed_on_pages H-ID\"",
                                                           "\"last_observed_in_doc H-ID\"", "is_unobserved",
                                                           "claim_freetext", "preferred_siglum", "alternative_sigla",
                                                           "status_witness", "status_notes", "is_excerpt",
                                                           "\"regional_writing_style H-ID\"", "scripta_freetext",
                                                           "date_of_creation", "date_of_creation_certainty",
                                                           "date_of_creation_source", "date_freetext",
                                                           "\"scribe H-ID\"", "number_of_hands", "scribe_note",
                                                           "\"place_of_creation H-ID\"", "place_of_creation_source"]
                      },
                  {"name_table": "TextTable", "attributes": ["\"H-ID\"", "preferred_name", "language_COLUMN",
                                                             "literary_form", "is_hypothetical", "claim_freetext",
                                                             "length", "length_freetext", "verse_type", "rhyme_type",
                                                             "stanza_type", "\"is_derived_from H-ID\"",
                                                             "nature_of_derivations", "tradition_status",
                                                             "status_notes", "\"in_stemma H-ID\"",
                                                             "\"regional_writing_style H-ID\"", "scripta_freetext",
                                                             "date_of_creation", "date_of_creation_certainty",
                                                             "date_of_creation_source", "date_freetext",
                                                             "\"is_written_by H-ID\"", "\"is_adapted_by H-ID\"",
                                                             "author_freetext", "\"place_of_creation H-ID\"",
                                                             "place_of_creation_source"]},
                  {"name_table": "Genre", "attributes": ["\"H-ID\"", "preferred_name"],
                            "recursives": ["parent_genre H-ID"]},
                  {"name_table": "Story", "attributes": ["\"H-ID\"", "preferred_name",
                                                         "\"is_part_of_storyverse H-ID\""]}
                  ]
        joins = [{"type_join": "LEFT JOIN", "table": "TextTable",
                  "on": "ON Witness.\"is_manifestation_of H-ID\" = TextTable.\"H-ID\" "},
                 {"type_join": "LEFT JOIN", "table": "Genre",
                  "on": "ON TextTable.\"specific_genre H-ID\" = Genre.\"H-ID\" "},
                 {"type_join": "LEFT JOIN UNNEST(TextTable.\"is_expression_of H-ID\") AS s(story_id) ON TRUE LEFT JOIN",
                  "table": "Story", "on": "ON Story.\"H-ID\" = s.story_id "}
                 ]
        condition = ""
        if languages:
            if isinstance(languages, str):
                languages = [languages]
            condition = f"WHERE TextTable.language_COLUMN IN ('{"', '".join(languages)}')"
        result = self.table("Witness", condition, joins, select)
        to_solve = {"Place": {"attributes": ["\"H-ID\"", "place_name", "administrative_region", "country"],
                              "name_joins": ["Witness_place_of_creation H-ID", "TextTable_place_of_creation H-ID"]},
                    "Person": {"attributes": ["\"H-ID\"", "given_names", "family_name", "floruit", "date_of_birth",
                                              "date_of_death"],
                               "name_joins": ["Witness_scribe H-ID", "TextTable_is_written_by H-ID",
                                              "TextTable_is_adapted_by H-ID"]},
                    "Stemma": {"attributes": ["\"H-ID\"", "\"openstemmata id\""],
                               "name_joins": ["TextTable_in_stemma H-ID"]},
                    "Scripta": {"attributes": ["\"H-ID\"", "preferred_name", "language_COLUMN"],
                                "name_joins": ["TextTable_regional_writing_style H-ID",
                                               "Witness_regional_writing_style H-ID"]},
                    "Storyverse": {"attributes": ["\"H-ID\"", "preferred_name"],
                                   "name_joins": ["Story_is_part_of_storyverse H-ID"]},
                    "DocumentTable": {"attributes": ["\"H-ID\"", "\"location H-ID\"", "collection", "current_shelfmark",
                                                     "invented_label"],
                                      "name_joins": ["Witness_last_observed_in_doc H-ID"],
                                      "depends_on": {"Repository": {"attributes": ["\"H-ID\"", "label_name",
                                                                                   "\"city H-ID\""],
                                                                    "name_joins": ["DocumentTable_location H-ID"]},
                                                     "Place": {"attributes": ["\"H-ID\"", "place_name"],
                                                               "name_joins": ["Repository_city H-ID"]}
                                                     }
                                      }
                    }
        result = self._solving_tables(result, to_solve)
        # simplify some data
        for col in result.columns:
            if result[col].apply(lambda x: isinstance(x, dict)).any():
                result[col] = result[col].apply(normalize_heurist_date)
        if drop_empty_columns:
            kwargs = {}
            if columns_to_keep is not None:
                kwargs["to_keep_anyway"] = columns_to_keep
            if completeness_threshold is not None:
                kwargs["threshold"] = completeness_threshold
            result = too_empty_columns(result, **kwargs)
        return result

    def parts(self, languages: list | str = None,
              columns_to_keep: list = None,
              completeness_threshold: float = None,
              drop_empty_columns: bool = True):
        """
        Return a selection of attributes of the part table and his linked tables
            Filter on the language_COLUMN text attribute (ex: 'dum (Middle Dutch)')
        """
        select = [{"name_table": "Part", "attributes": ["\"H-ID\"", "div_order", "page_ranges"]},
                  {"name_table": "DocumentTable", "attributes": ["\"H-ID\"", "current_shelfmark", "collection",
                                                                 "location_known", "location_notes",
                                                                 "collection_of_fragments", "old_shelfmark",
                                                                 "digitization_freetext"]},
                  {"name_table": "Digitization", "attributes": ["\"H-ID\"", "URI"]},
                  {"name_table": "Repository", "attributes": ["\"H-ID\"", "preferred_name", "label_name", "VIAF",
                                                              "\"city H-ID\""]}]
        joins = [{"type_join": "LEFT JOIN", "table": "DocumentTable",
                  "on": "ON Part.\"is_inscribed_on H-ID\" = DocumentTable.\"H-ID\" "},
                 {"type_join": "LEFT JOIN", "table": "Repository",
                  "on": "ON DocumentTable.\"location H-ID\" = Repository.\"H-ID\" "},
                 {"type_join": "LEFT JOIN ("
                               "SELECT d.*, u.doc_id "
                               "FROM Digitization d " 
                               "CROSS JOIN UNNEST(d.\"digitization_of H-ID\") AS u(doc_id) "
                               "WHERE d.is_deprecated = FALSE "
                               "QUALIFY row_number() OVER ("
                                    "PARTITION BY u.doc_id  "
                                    "ORDER BY d.\"H-ID\" ASC"
                                    ") = 1 "
                               ") Digitization ",
                  "on": "ON DocumentTable.\"H-ID\" = Digitization.doc_id "}]
        condition = ""
        if languages:
            if isinstance(languages, str):
                languages = [languages]
            condition += f"""
                        WHERE EXISTS (
                            SELECT 1
                            FROM Witness
                            CROSS JOIN UNNEST(Witness."observed_on_pages H-ID") AS p(part_id)
                            JOIN TextTable ON Witness.\"is_manifestation_of H-ID\" = TextTable.\"H-ID\"
                            WHERE p.part_id = Part.\"H-ID\"
                              AND TextTable.language_COLUMN IN ('{"', '".join(languages)}')
                        )
                        """
        result = self.table("Part", condition, joins, select)
        to_solve = {"Place": {"attributes": ["\"H-ID\"", "place_name", "administrative_region", "country"],
                              "name_joins": ["Repository_city H-ID"]}}
        result = self._solving_tables(result, to_solve)
        for col in result.columns:
            if result[col].apply(lambda x: isinstance(x, dict)).any():
                result[col] = result[col].apply(normalize_heurist_date)
        if drop_empty_columns:
            kwargs = {}
            if columns_to_keep is not None:
                kwargs["to_keep_anyway"] = columns_to_keep
            if completeness_threshold is not None:
                kwargs["threshold"] = completeness_threshold
            result = too_empty_columns(result, **kwargs)
        return result

    def stories(self, languages: list | str = None):
        """
        Return the content of the story table connected to the storyverse table
        """
        select = [{"name_table": "Story", "attributes": ["\"H-ID\"", "preferred_name"]},
                  {"name_table": "Storyverse", "attributes": ["\"H-ID\"", "preferred_name"],
                   "recursives": ["member_of_cycle H-ID"]}
                  ]
        joins = [{"type_join": "LEFT JOIN UNNEST(Story.\"is_part_of_storyverse H-ID\") AS sv(storyverse_id) "
                               "ON TRUE LEFT JOIN",
                  "table": "Storyverse", "on": "ON Storyverse.\"H-ID\" = sv.storyverse_id "}]
        condition = ""
        if languages:
            if isinstance(languages, str):
                languages = [languages]
            condition += f"""
            WHERE EXISTS (
                SELECT 1
                FROM TextTable
                CROSS JOIN UNNEST(TextTable."is_expression_of H-ID") AS s(story_id)
                WHERE s.story_id = Story.\"H-ID\"
                  AND TextTable.language_COLUMN IN ('{"', '".join(languages)}')
            )
            """
        result = self.table("Story", condition, joins, select)
        return result

    def overview(self, languages: list | str = None) -> pd.DataFrame:
        """
        Summary the fill rates of each column in general output
        """
        witnesses = self.witnesses(languages=languages, drop_empty_columns=False)
        parts = self.parts(languages=languages, drop_empty_columns=False)
        if not languages:
            languages = ["dum (Middle Dutch)", "enm (Middle English)", "non_WEST (West Old Norse)",
                         "non_EAST (East Old Norse)", "fro_PRO (Franco-Occitan)", "frm (Middle French)",
                         "frp (Franco-Provençal)", "pro (Occitan)", "fro (Old French)", "fro_ITA (Franco-Italian)",
                         "fro_ENG (Anglo-Norman)", "lat (Latin)", "gmh (Middle High German)", "gml (Middle Low German)",
                         "cat (Catalan)", "glg (Galician)", "glg_POR (Galician-Portugese)", "por (Portugese)",
                         "spa (Spanish)", "ita (Italian)", "ghg (Early Modern Irish)", "mga (Middle Irish)",
                         "oco (Old Cornish)", "wlm (Middle Welsh)"]
        fill_rates = pd.DataFrame()
        fill_rates["total"] = pd.concat([pd.Series({"nbr_witnesses": len(witnesses)}),
                                         too_empty_columns(witnesses, drop=False),
                                         too_empty_columns(parts, drop=False)])
        witness_to_join = ((witnesses[["TextTable_language_COLUMN", "Witness_observed_on_pages H-ID"]]
                           .explode("Witness_observed_on_pages H-ID"))
                           .rename(columns={"Witness_observed_on_pages H-ID": "Part_H-ID"})
                           .dropna(subset=["Part_H-ID"]))
        parts = parts.merge(
            witness_to_join,
            on="Part_H-ID",
            how="left"
        )
        for language in languages:
            witnesses_filtred = witnesses[witnesses.TextTable_language_COLUMN == language]
            if not witnesses_filtred.empty:
                parts_filtred = parts[parts.TextTable_language_COLUMN == language]
                parts_filtred = parts_filtred.drop(columns=["TextTable_language_COLUMN"])
                fill_rates[language] = pd.concat([pd.Series({"nbr_witnesses": len(witnesses_filtred)}),
                                                  too_empty_columns(witnesses_filtred, drop=False),
                                                  too_empty_columns(parts_filtred, drop=False)])
        return fill_rates

    def analyse(self, name_table: str = None,
                language: str = None) -> dict | str:
        """
        A function to analyze the completeness of each table for each corpus
        """
        if name_table[0].isupper():
            name_table = name_table[0].lower() + name_table[1:]
        sql_name = LOSTMA_TABLES[name_table]["safe_sql_name"]
        self._is_table_exists(name_table, sql_name)
        kwargs = {}
        if language and "language_filter" in LOSTMA_TABLES[name_table].keys():
            kwargs["condition"] = LOSTMA_TABLES[name_table]["language_filter"]
            kwargs["language"] = language
        result = self.table(sql_name, **kwargs)
        len_table = len(result)
        if len_table:
            action_required = "No data for this table"
            if "review_status" in result.columns:
                action_required = len(result[result["review_status"] == "Action required"])
            requirements = self._get_requirements(sql_name)
            keep_cols = result.columns.tolist()
            for column in result.columns:
                if column in ["H-ID", "type_id"] or "TRM-ID" in column or requirements.get(column) is None:
                    keep_cols.remove(column)
            result = result[keep_cols]
            result = empty_lists_to_na(result)
            result_analyse = pd.DataFrame({
                "requirements statement": requirements,
                "empty records": result.isna().sum(),
                "percentage empty": round((result.isnull().mean() / len_table) * 100, 2)
            })
            summary = pd.DataFrame({
                "value": [len_table, action_required]
            }, index=["total", "action_required"])
            return {"summary": summary, "data": result_analyse}
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