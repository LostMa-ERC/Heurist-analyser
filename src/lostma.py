import duckdb
import pandas as pd
from main import def_requirements, BASE_TABLES, schema_path, duck_db_path

"""
Ce fichier sert  juste à utiliser python pour faire des requêtes sur les corpus chargés dans la Duck DB

L'enjeu de base est de récolter les données chargées pour chaque corpus linguistique de Lostma
J'ai donc construit des fonctions accompagnées de requêtes, spécifique pour chaque table, qui va chercher les données
en fonction du corpus linguistique.

Ce fichier abâtardit des fonctions qui se voulaient génériques construites dans le fichier main.
Même si elle est peu satisfaisante, cette solution permet au moins de récolter les informations dont on a besoin

Idéalement, il faudrait un jour parvenir à modifier cela pour le rendre disponible au sein de l'Heurist-API
Cela prendrait la forme d'une fonction qui ferait des requête pour valider la complétude des données, mais qui pourrait
aussi sélectionner des sous-corpus en fonction de la valeur d'un attribut pour affiner cette analyse

PS : Attention au fait que l'Heurist-API ne télécharge par défaut que les données de la catégorie 'My record types'
"""

with duckdb.connect(duck_db_path) as con:
    query = con.sql(f"SELECT * FROM TextTable WHERE language_COLUMN = 'fro (Old French)';")
    query.fetchdf().to_csv("result.csv")

def collect_presence_specific_data(
    column_names: list[dict | str] = None,
    lang: str = None
) -> tuple[dict, str]:
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
                len_table = con.sql(f"SELECT COUNT(*) FROM {name_table} WHERE language_COLUMN = '{lang}';").fetchone()[0]
                collect[name_table] = {}
                details = [d for l in table.values() for d in l if d not in ["H-ID", "type_id"] and "TRM-ID" not in d]
                for detail in details:
                    if detail in required_data[name_table]:
                        req_type = required_data[name_table][detail]
                        dtype = con.execute("""
                                SELECT data_type
                                FROM information_schema.columns
                                WHERE table_name = ?
                                  AND column_name = ?
                            """, [name_table, detail]).fetchone()[0]
                        if dtype.endswith('[]'):
                            # Cas liste
                            query = (f"SELECT count (*) FROM {name_table} WHERE (\"{detail}\" IS NULL "
                                     f"OR array_length(\"{detail}\") = 0) "
                                     f"AND language_COLUMN = '{lang}'")
                        else:
                            # Cas scalaire
                            query = (f"SELECT count (*) FROM {name_table} WHERE \"{detail}\" IS NULL "
                                     f"AND language_COLUMN = '{lang}'")
                        count_empty = con.sql(query).fetchone()[0]
                        collect[name_table][detail] = {'required statement' : req_type,
                                                       'empty records': count_empty,
                                                       'percentage empty': round((count_empty / len_table) * 100, 2),
                                                       'total records': len_table}
                    else:
                        log_data.append(f"{name_table}.{detail}")
            log_return = f"fields {", ".join(log_data)} are hidden"
    return collect, log_return

"""
Requêtes pour les text :
1 - len_table = con.sql(f"SELECT COUNT(*) FROM {name_table} WHERE language_COLUMN = '{lang}';").fetchone()[0]
2 - query = f"SELECT count (*) FROM {name_table} WHERE \"{detail}\" IS NULL AND language_COLUMN = '{lang}'"
3 - action_required = con.sql(f"SELECT count(*) FROM {table} WHERE review_status = 'Action required' AND language_COLUMN = '{language}'")

Requêtes pour les witness :
1 - len_table = con.sql(f"SELECT COUNT(*) FROM {name_table} "
                                    f"INNER JOIN TextTable ON TextTable.\"H-ID\" = {name_table}.\"is_manifestation_of H-ID\""
                                    f"WHERE TextTable.language_COLUMN = '{lang}';").fetchone()[0]
2 - query = (f"SELECT count (*) FROM {name_table} "
                                 f"INNER JOIN TextTable ON TextTable.\"H-ID\" = {name_table}.\"is_manifestation_of H-ID\""
                                 f"WHERE {name_table}.\"{detail}\" IS NULL AND TextTable.language_COLUMN = '{lang}'")
3 - action_required = con.sql(f"SELECT count(*) FROM {table} "
                              f"INNER JOIN TextTable ON TextTable.\"H-ID\" = {table}.\"is_manifestation_of H-ID\""
                              f"WHERE {table}.review_status = 'Action required' AND TextTable.language_COLUMN = '{language}'")

Requêtes pour les part :
1 - len_table = con.sql(f"SELECT COUNT(DISTINCT {name_table}.\"H-ID\") FROM {name_table} "
                                    f"INNER JOIN witness ON True "
                                    f"INNER JOIN UNNEST(witness.\"observed_on_pages H-ID\") AS w ON w.unnest = {name_table}.\"H-ID\""
                                    f"INNER JOIN TextTable ON TextTable.\"H-ID\" = witness.\"is_manifestation_of H-ID\" "
                                    f"WHERE TextTable.language_COLUMN = '{lang}';").fetchone()[0]
2 - query = (f"SELECT count (DISTINCT {name_table}.\"H-ID\") FROM {name_table} "
                                 f"INNER JOIN witness ON True "
                                 f"INNER JOIN UNNEST(witness.\"observed_on_pages H-ID\") AS w ON w.unnest = {name_table}.\"H-ID\""
                                 f"INNER JOIN TextTable ON TextTable.\"H-ID\" = witness.\"is_manifestation_of H-ID\""
                                 f"WHERE {name_table}.\"{detail}\" IS NULL AND TextTable.language_COLUMN = '{lang}'")
3 - action_required = con.sql(f"SELECT count(DISTINCT {table}.\"H-ID\") FROM {table} "
                              f"INNER JOIN witness ON True "
                              f"INNER JOIN UNNEST(witness.\"observed_on_pages H-ID\") AS w ON w.unnest = {table}.\"H-ID\""
                              f"INNER JOIN TextTable ON TextTable.\"H-ID\" = witness.\"is_manifestation_of H-ID\""
                              f"WHERE {table}.review_status = 'Action required' AND TextTable.language_COLUMN = '{language}'")
                              
Requêtes pour les document :
1 - len_table = con.sql(f"SELECT COUNT(DISTINCT {name_table}.\"H-ID\") FROM {name_table} "
                                    f"INNER JOIN part ON part.\"is_inscribed_on H-ID\" = {name_table}.\"H-ID\""
                                    f"INNER JOIN witness ON True "
                                    f"INNER JOIN UNNEST(witness.\"observed_on_pages H-ID\") AS w ON w.unnest = part.\"H-ID\""
                                    f"INNER JOIN TextTable ON TextTable.\"H-ID\" = witness.\"is_manifestation_of H-ID\" "
                                    f"WHERE TextTable.language_COLUMN = '{lang}';").fetchone()[0]
2 - query = (f"SELECT count (DISTINCT {name_table}.\"H-ID\") FROM {name_table} "
                                 f"INNER JOIN part ON part.\"is_inscribed_on H-ID\" = {name_table}.\"H-ID\""
                                 f"INNER JOIN witness ON True "
                                 f"INNER JOIN UNNEST(witness.\"observed_on_pages H-ID\") AS w ON w.unnest = part.\"H-ID\""
                                 f"INNER JOIN TextTable ON TextTable.\"H-ID\" = witness.\"is_manifestation_of H-ID\""
                                 f"WHERE {name_table}.\"{detail}\" IS NULL AND TextTable.language_COLUMN = '{lang}'")
3 - action_required = con.sql(f"SELECT count(DISTINCT {table}.\"H-ID\") FROM {table} "
                              f"INNER JOIN part ON part.\"is_inscribed_on H-ID\" = {table}.\"H-ID\""
                              f"INNER JOIN witness ON True "
                              f"INNER JOIN UNNEST(witness.\"observed_on_pages H-ID\") AS w ON w.unnest = part.\"H-ID\""
                              f"INNER JOIN TextTable ON TextTable.\"H-ID\" = witness.\"is_manifestation_of H-ID\""
                              f"WHERE {table}.review_status = 'Action required' AND TextTable.language_COLUMN = '{language}'")

Requêtes pour les digitiation :
1 - len_table = con.sql(f"SELECT COUNT(DISTINCT {name_table}.\"H-ID\") FROM {name_table} "
                                    f"INNER JOIN DocumentTable ON True "
                                    f"INNER JOIN UNNEST(DocumentTable.\"digitization H-ID\") AS d ON d.unnest = {name_table}.\"H-ID\""
                                    f"INNER JOIN part ON part.\"is_inscribed_on H-ID\" = DocumentTable.\"H-ID\""
                                    f"INNER JOIN witness ON True "
                                    f"INNER JOIN UNNEST(witness.\"observed_on_pages H-ID\") AS w ON w.unnest = part.\"H-ID\""
                                    f"INNER JOIN TextTable ON TextTable.\"H-ID\" = witness.\"is_manifestation_of H-ID\" "
                                    f"WHERE TextTable.language_COLUMN = '{lang}';").fetchone()[0]
2 - query = (f"SELECT count (DISTINCT {name_table}.\"H-ID\") FROM {name_table} "
                                 f"INNER JOIN DocumentTable ON True "
                                 f"INNER JOIN UNNEST(DocumentTable.\"digitization H-ID\") AS d ON d.unnest = {name_table}.\"H-ID\""
                                 f"INNER JOIN part ON part.\"is_inscribed_on H-ID\" = DocumentTable.\"H-ID\""
                                 f"INNER JOIN witness ON True "
                                 f"INNER JOIN UNNEST(witness.\"observed_on_pages H-ID\") AS w ON w.unnest = part.\"H-ID\""
                                 f"INNER JOIN TextTable ON TextTable.\"H-ID\" = witness.\"is_manifestation_of H-ID\""
                                 f"WHERE {name_table}.\"{detail}\" IS NULL AND TextTable.language_COLUMN = '{lang}'")
3 - Pas de review_status pour ce champ

Requêtes pour les physDesc :
1 - len_table = con.sql(f"SELECT COUNT(DISTINCT {name_table}.\"H-ID\") FROM {name_table} "
                                    f"INNER JOIN part ON part.\"physical_description H-ID\" = {name_table}.\"H-ID\""
                                    f"INNER JOIN witness ON True "
                                    f"INNER JOIN UNNEST(witness.\"observed_on_pages H-ID\") AS w ON w.unnest = part.\"H-ID\""
                                    f"INNER JOIN TextTable ON TextTable.\"H-ID\" = witness.\"is_manifestation_of H-ID\" "
                                    f"WHERE TextTable.language_COLUMN = '{lang}';").fetchone()[0]
2 - query = (f"SELECT count (DISTINCT {name_table}.\"H-ID\") FROM {name_table} "
                                 f"INNER JOIN part ON part.\"physical_description H-ID\" = {name_table}.\"H-ID\""
                                 f"INNER JOIN witness ON True "
                                 f"INNER JOIN UNNEST(witness.\"observed_on_pages H-ID\") AS w ON w.unnest = part.\"H-ID\""
                                 f"INNER JOIN TextTable ON TextTable.\"H-ID\" = witness.\"is_manifestation_of H-ID\""
                                 f"WHERE {name_table}.\"{detail}\" IS NULL AND TextTable.language_COLUMN = '{lang}'")
3 - action_required = con.sql(f"SELECT count(DISTINCT {table}.\"H-ID\") FROM {table} "
                              f"INNER JOIN part ON part.\"physical_description H-ID\" = {table}.\"H-ID\""
                              f"INNER JOIN witness ON True "
                              f"INNER JOIN UNNEST(witness.\"observed_on_pages H-ID\") AS w ON w.unnest = part.\"H-ID\""
                              f"INNER JOIN TextTable ON TextTable.\"H-ID\" = witness.\"is_manifestation_of H-ID\""
                              f"WHERE {table}.review_status = 'Action required' AND TextTable.language_COLUMN = '{language}'")

Requêtes pour les stemma :
1 - len_table = con.sql(f"SELECT count(DISTINCT {name_table}.\"H-ID\") FROM {name_table} "
                                    f"INNER JOIN TextTable ON True "
                                    f"INNER JOIN UNNEST(TextTable.\"in_stemma H-ID\") AS s ON s.unnest = {name_table}.\"H-ID\""
                                    f"WHERE TextTable.language_COLUMN = '{lang}';").fetchone()[0]
2 - query = (f"SELECT count(DISTINCT {name_table}.\"H-ID\") FROM {name_table} "
                                 f"INNER JOIN TextTable ON True "
                                 f"INNER JOIN UNNEST(TextTable.\"in_stemma H-ID\") AS s ON s.unnest = {name_table}.\"H-ID\""
                                 f"WHERE {name_table}.\"{detail}\" IS NULL AND TextTable.language_COLUMN = '{lang}'")
3 - Pas de review_status pour ce champ

Requêtes pour les scripta :
1 - len_table = con.sql(f"SELECT count(DISTINCT {name_table}.\"H-ID\") FROM {name_table} "
                                    f"WHERE {name_table}.language_COLUMN = '{lang}'").fetchone()[0]
2 - query = (f"SELECT count(DISTINCT {name_table}.\"H-ID\") FROM {name_table} "
                                 f"WHERE {name_table}.\"{detail}\" IS NULL AND {name_table}.language_COLUMN = '{lang}'")
3 - action_required = con.sql(f"SELECT count(DISTINCT {table}.\"H-ID\") FROM {table} "
                                  f"WHERE {table}.review_status = 'Action required' AND {table}.language_COLUMN = '{language}'")

Requêtes pour les autres tables:
1 - len_table = con.sql(f"SELECT count(DISTINCT {name_table}.\"H-ID\") FROM {name_table}").fetchone()[0]
2 - query = (f"SELECT count(DISTINCT {name_table}.\"H-ID\") FROM {name_table} "
             f"WHERE {name_table}.\"{detail}\" IS NULL")
3 - action_required = con.sql(f"SELECT count(DISTINCT {table}.\"H-ID\") FROM {table} "
                              f"WHERE {table}.review_status = 'Action required'")
"""

languages = ["dum (Middle Dutch)", "enm (Middle English)", "non (Old Norse)", "fro_PRO (Franco-Occitan)", "frm (Middle French)", "frp (Franco-Provençal)", "pro (Occitan)",
             "fro (Old French)", "fro_ITA (Franco-Italian)", "fro_ENG (Anglo-Norman)", "lat (Latin)", "gmh (Middle High German)", "ita (Italian)"]

# J'ai supprimé la boucle linguistique pour les dernières tables
table = "TextTable"

for language in languages:
    result = collect_presence_specific_data([table], lang=language)[0]
    print(language)
    for r in result.keys():
        df = pd.DataFrame.from_dict(result[r], orient="index")
        df.to_csv(f"result-{language}.csv")
        # Cette donnée n'est présente que pour certaines tables
        con = duckdb.connect(duck_db_path)
        action_required = con.sql(f"SELECT count(*) FROM {table} WHERE review_status = 'Action required' AND language_COLUMN = '{language}'")
        print(action_required)