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
        self.duckdb_path = Path(duckdb_path) if duckdb_path else base / "lostma.duckdb"
        self.schema_dir = Path(self.database + "_schema")

    def download_database(self) -> None:
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
              ]
        subprocess.run(cmd, check=True)

    def download_schema(self) -> None:
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
        ]
        subprocess.run(cmd, check=True)

    def sync(self) -> None:
        """
        Download the db and its schema
        """
        self.download_database()
        self.download_schema()

    def sql(self, query: str, params: list = None):
        """
        Execute a request and return a dataframe
        """
        con = duckdb.connect(self.duckdb_path)
        res = con.execute(query, params).fetchdf()
        con.close()
        return res

    def texts(self, languages: list = None):
        """
        Return the content of the text table

        :param languages: list, optional
            Filter on the language_COLUMN attribute (ex: 'dum (Middle Dutch)')
        """

        query = "SELECT * FROM TextTable "
        if languages:
            query += f"WHERE language_COLUMN IN ('{"', '".join(languages)}')"
        return self.sql(query)

    def witnesses(self, languages: list = None):
        """
        Return the content of the witness table

        :param languages: list, optional
            Filter on the language_COLUMN text attribute (ex: 'dum (Middle Dutch)')
        """

        query = ("SELECT * FROM witness "
                 "LEFT JOIN TextTable ON witness.\"is_manifestation_of H-ID\" = TextTable.\"H-ID\" ")
        if languages:
            if isinstance(languages, str):
                languages = [languages]
            query += f"WHERE TextTable.language_COLUMN IN ('{"', '".join(languages)}')"
        return self.sql(query)

    def analyse(self, name_table: str = None,
                language: str = None):
        """

        :param language: str
        :param name_table: str
        :return: tuple
        """
        if name_table[0].isupper():
            name_table = name_table[0].lower() + name_table[1:]
        sql_name = LOSTMA_TABLES[name_table]["safe_sql_name"]
        columns = self.sql("SELECT column_name FROM information_schema.columns "
                           "WHERE table_name = ?;", [sql_name])["column_name"].tolist()
        required_data = def_requirements(self.schema_dir)
        action_required = "No field for this table"
        if LOSTMA_TABLES[name_table]["is_corpus_data"]:
            len_table = self.sql(LOSTMA_TABLES[name_table]["len_query"], [language]).iloc[0, 0]
            if len_table and LOSTMA_TABLES[name_table]["action_required"]:
                action_required = self.sql(LOSTMA_TABLES[name_table]["action_required"], [language]).iloc[0, 0]
        else:
            len_table = self.sql(LOSTMA_TABLES["non-corpus tables"]["len_query"].format(table=sql_name)).iloc[0, 0]
            if len_table and LOSTMA_TABLES[name_table]["is_action_required"]:
                action_required = self.sql(LOSTMA_TABLES["non-corpus tables"]["action_required"].format(table=sql_name)
                                           ).iloc[0, 0]
        list_empty = []
        if len_table:
            for column in columns:
                if column not in ["H-ID", "type_id"] and "TRM-ID" not in column and column in required_data[sql_name]:
                    req_type = required_data[sql_name][column]
                    dtype = self.sql("SELECT data_type FROM information_schema.columns "
                                     "WHERE table_name = ? AND column_name = ?", [sql_name, column]).iloc[0, 0]
                    if dtype.endswith('[]'):
                        # checks whether the data is a list
                        if LOSTMA_TABLES[name_table]["is_corpus_data"]:
                            count_empty = self.sql(LOSTMA_TABLES[name_table]["list_query"].format(detail=column),
                                                   [language]).iloc[0, 0]
                        else:
                            count_empty = self.sql(LOSTMA_TABLES["non-corpus tables"]["list_query"].format(
                                detail=column, table=sql_name)).iloc[0, 0]
                    else:
                        if LOSTMA_TABLES[name_table]["is_corpus_data"]:
                            count_empty = self.sql(LOSTMA_TABLES[name_table]["scalar_query"].format(detail=column),
                                                   [language]).iloc[0, 0]
                        else:
                            count_empty = self.sql(LOSTMA_TABLES["non-corpus tables"]["scalar_query"].format(
                                detail=column, table=sql_name)).iloc[0, 0]
                    list_empty.append({'field': column,
                                       'required statement': req_type,
                                       'empty records': count_empty,
                                       'percentage empty': round((count_empty / len_table) * 100, 2)})
            return {
                "completeness table": pd.DataFrame(list_empty),
                "total records": len_table,
                "action required": action_required
            }
        else:
            return "No data"