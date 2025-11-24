import subprocess
import duckdb

class LostmaDB:
    def __init__(self, login, password):
        self.database = "jbcamps_gestes"
        self.duckdb_path = "lostma.db"
        self.login = login
        self.password = password
        self.cli_path = "heurist"

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

    def sql(self, query: str):
        """
        Execute a request and return a dataframe
        """
        con = duckdb.connect(self.duckdb_path)
        res = con.execute(query).fetchdf()
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

        query = "SELECT witness.* FROM witness LEFT JOIN text ON witness.\"is_manifestation of H-ID\" = text.\"H-ID\""
        if languages:
            if isinstance(languages, str):
                languages = [languages]
            query += f"WHERE language_COLUMN IN ('{"', '".join(languages)}')"
        return self.sql(query)