from pathlib import Path
import duckdb
import csv
import ast
from .background import yield_log_blocks
from.general import def_requirements, KEYWORDS

"""
Here are some general check-up functions, not use for now in this library. But I want to keep them here
in order to use them later as part of the future of this function
"""

current_path = Path(__file__).parent.parent
VALIDATION_LOG = current_path.joinpath("validation.log")
schema_path = current_path.joinpath("jbcamps_gestes_schema")
duck_db_path = current_path.joinpath("lostma.db")
BASE_TABLES = [
        ("rtg", "RecTypeGroups"),
        ("rst", "RecStructure"),
        ("rty", "RecTypes"),
        ("dty", "DetailTypes"),
        ("trm", "Terms"),
    ]


def log_data() -> dict:
    # Analyse log data
    if VALIDATION_LOG.is_file():
        with open(VALIDATION_LOG) as f:
            log = f.readlines()
    else:
        log = []

    recs = {}
    for block in yield_log_blocks(log):
        if block.recType not in recs:
            recs[block.recType] = [block.recID]
        else:
            if block.recID not in recs[block.recType]:
                recs[block.recType].append(block.recID)
    return recs

def count_log() -> dict:
    # summary most problematic data with content of log file
    mistakes, collect_mistakes = log_data(), {}
    with duckdb.connect(duck_db_path) as con:
        tables = [t[0] for t in con.sql("show tables;").fetchall()]
        for table in tables:
            # Je pourrai ici aussi m'inspirer de la fonction safe_sql
            table = table[0].lower() + table[1:].replace("Table", "")
            if table not in [t[0] for t in BASE_TABLES]:
                table_id = con.sql(f"SELECT rty_ID FROM rty WHERE rty_Name = '{table}';").fetchone()[0]
                if table_id in mistakes:
                    len_table = con.sql(f"SELECT COUNT(*) FROM {table};").fetchone()[0]
                    count_mistakes = len(mistakes[table_id])
                    collect_mistakes[table] = {'records on log': count_mistakes,
                                               'total records': len_table,
                                               'percentage problem':round((count_mistakes / len_table) * 100, 2)}
    return collect_mistakes

# print(count_log())

def count_required_data() -> dict:
    # collect records with empty data on required fields
    required_data = def_requirements(schema_path, "required")
    collect = {}
    with duckdb.connect(duck_db_path) as con:
        for table in required_data:
            list_detail = []
            for detail in required_data[table]:
                if required_data[table][detail] == "required":
                    list_detail.append(f"\"{detail}\" IS NULL")
            if list_detail:
                query = f"FROM {table} WHERE "
                query += " OR ".join(list_detail)
                len_table = con.sql(f"SELECT COUNT(*) FROM {table};").fetchone()[0]
                count_empty = con.sql("SELECT count (*)" + query).fetchone()[0]
                collect[table] = {'empty required records': count_empty,
                                  'total records': len_table,
                                  'percentage problem': round((count_empty / len_table) * 100, 2)}
    return  collect

# print(count_required_data())

def collect_presence_data(
    column_names: list[dict | str] = None
) -> tuple[dict, str]:
    # summary presence of data in the Duck DB
    # note: il existe apparemment un moyen de scoper les tables en fonction d'un critère
    # puis de rendre récurcif ce scope sur les autres tables à partir desquelles elle est liée
    # ça ferait une étape supplémentaire préalable à l'utilisation de cette fonction sur
    # une view sql déjà préparée, mais on peut essayer de l'envisager
    collect, log_data = {}, []
    required_data = def_requirements(schema_path)
    with duckdb.connect(duck_db_path) as con:
        # if no input data, search for everything
        if not column_names:
            column_names = []
            tables = [t[0] for t in con.sql("show tables;").fetchall()]
            for table in tables:
                if table not in [t[0] for t in BASE_TABLES]:
                    name_columns = con.sql(f"SELECT column_name FROM information_schema.columns "
                                           f"WHERE table_name = '{table}';")
                    columns = [t[0] for t in name_columns.fetchall()]
                    column_names.append({table: columns})
        # if no precised column, search for everything in the table
        for table in column_names:
            if not isinstance(table, dict):
                # Je pourrai ici aussi m'inspirer de la fonction safe_sql
                table = table[0].upper() + table[1:]
                name_columns = con.sql(f"SELECT column_name FROM information_schema.columns "
                                       f"WHERE table_name = '{table}';")
                columns = [t[0] for t in name_columns.fetchall()]
                column_names.append({table: columns})
        for table in column_names:
            if isinstance(table, dict):
                name_table = [t for t in table.keys()][0]
                len_table = con.sql(f"SELECT COUNT(*) FROM {name_table};").fetchone()[0]
                collect[name_table] = {}
                details = [d for l in table.values() for d in l if d not in ["H-ID", "type_id"] and "TRM-ID" not in d]
                for detail in details:
                    if detail in required_data[name_table]:
                        req_type = required_data[name_table][detail]
                        query = f"SELECT count (*) FROM {name_table} WHERE \"{detail}\" IS NULL"
                        count_empty = con.sql(query).fetchone()[0]
                        collect[name_table][detail] = {'required statement' : req_type,
                                                       'empty records': count_empty,
                                                       'total records': len_table,
                                                       'percentage empty': round((count_empty / len_table) * 100, 2)}
                    else:
                        log_data.append(f"{name_table}.{detail}")
            log_return = f"fields {", ".join(log_data)} are hidden"
    return collect, log_return

# print(collect_presence_data(["witness"]))

def def_enum(
        path: Path | str,
) -> dict:
    # Import the predefined vocabulary of fields from the schema of tables
    data = {}
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
                    if row['dty_Type'] == "enum":
                        vocab_terms = [t.split("={")[0].replace("{", "").replace("\'", "").replace("\\\\\\", "\'")
                                       for t in row['vocabTerms'].split("}, ")]
                        data[file_name][name_detail] = vocab_terms
    return data

def validation_enum() -> dict:
    # summary presence of un undesired data in fields with predefined vocabulary
    collect = {}
    enums = def_enum(schema_path)
    with duckdb.connect(duck_db_path) as con:
        for table in enums:
            enums_table = enums[table]
            len_table = con.sql(f"SELECT COUNT(*) FROM {table};").fetchone()[0]
            if enums_table:
                for enum in enums_table:
                    data = con.sql(f"SELECT {enum} FROM {table} WHERE {enum} IS NOT NULL;").fetchall()
                    # J'ajoute ce filtre car DUckDB comprend le chanp "reference stemma" comme un integer
                    if data:
                        if con.sql(f"SELECT typeof({enum}) FROM {table}").fetchone()[0] == "VARCHAR[]":
                            query = (f"SELECT {enum} FROM {table} WHERE EXISTS ("
                                     f"SELECT 1 FROM UNNEST({table}.{enum}) AS x "
                                     f"WHERE x.unnest NOT IN (SELECT * FROM UNNEST(?) AS a(v)));")
                        else:
                            query = f"SELECT {enum} FROM {table} WHERE {enum} NOT IN (SELECT * FROM UNNEST(?) AS a(v));"
                        result = [t[0] for t in con.sql(query, params=[enums_table[enum]]).fetchall()]
                        count_mistake = len(result)
                        if result:
                            collect[table][enum] = {'empty records': count_mistake,
                                                    'total records': len_table,
                                                    'percentage problem': round((count_mistake / len_table) * 100, 2)}
    return collect

# print(validation_enum())