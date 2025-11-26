LOSTMA_TABLES = {
    "text": {
        "safe_sql_name": "TextTable",
        "is_corpus_data": True,
        "len_query": "SELECT COUNT(*) FROM TextTable WHERE language_COLUMN = ?;",
        "scalar_query": "SELECT count (*) FROM TextTable WHERE TextTable.\"{detail}\" IS NULL AND language_COLUMN = ?;",
        "list_query": "SELECT count (*) FROM TextTable "
                      "WHERE (TextTable.\"{detail}\" IS NULL OR array_length(TextTable.\"{detail}\") = 0) "
                      "AND language_COLUMN = ?",
        "action_required": "SELECT count(*) FROM TextTable "
                           "WHERE review_status = 'Action required' AND language_COLUMN = ?;"
    },
    "witness": {
            "safe_sql_name": "Witness",
            "is_corpus_data": True,
            "len_query": "SELECT COUNT(*) FROM witness "
                         "INNER JOIN TextTable ON TextTable.\"H-ID\" = witness.\"is_manifestation_of H-ID\" "
                         "WHERE TextTable.language_COLUMN = ?;",
            "scalar_query": "SELECT count (*) FROM witness "
                            "INNER JOIN TextTable ON TextTable.\"H-ID\" = witness.\"is_manifestation_of H-ID\" "
                            "WHERE witness.\"{detail}\" IS NULL AND TextTable.language_COLUMN = ?;",
            "list_query": "SELECT count (*) FROM witness "
                          "INNER JOIN TextTable ON TextTable.\"H-ID\" = witness.\"is_manifestation_of H-ID\" "
                          "WHERE (witness.\"{detail}\" IS NULL "
                          "OR array_length(witness.\"{detail}\") = 0) AND TextTable.language_COLUMN = ?;",
            "action_required": "SELECT count(*) FROM witness "
                               "INNER JOIN TextTable ON TextTable.\"H-ID\" = witness.\"is_manifestation_of H-ID\" "
                               "WHERE witness.review_status = 'Action required' AND TextTable.language_COLUMN = ?;"
        },
    "part": {
            "safe_sql_name": "Part",
            "is_corpus_data": True,
            "len_query": "SELECT COUNT(DISTINCT part.\"H-ID\") FROM part "
                         "INNER JOIN witness ON True "
                         "INNER JOIN UNNEST(witness.\"observed_on_pages H-ID\") AS w ON w.unnest = part.\"H-ID\" "
                         "INNER JOIN TextTable ON TextTable.\"H-ID\" = witness.\"is_manifestation_of H-ID\" "
                         "WHERE TextTable.language_COLUMN = ?;",
            "scalar_query": "SELECT count (DISTINCT part.\"H-ID\") FROM part "
                            "INNER JOIN witness ON True "
                            "INNER JOIN UNNEST(witness.\"observed_on_pages H-ID\") AS w ON w.unnest = part.\"H-ID\" "
                            "INNER JOIN TextTable ON TextTable.\"H-ID\" = witness.\"is_manifestation_of H-ID\" "
                            "WHERE part.\"{detail}\" IS NULL AND TextTable.language_COLUMN = ?;",
            "list_query": "SELECT count (DISTINCT part.\"H-ID\") FROM part "
                          "INNER JOIN witness ON True "
                          "INNER JOIN UNNEST(witness.\"observed_on_pages H-ID\") AS w ON w.unnest = part.\"H-ID\" "
                          "INNER JOIN TextTable ON TextTable.\"H-ID\" = witness.\"is_manifestation_of H-ID\" "
                          "WHERE (part.\"{detail}\" IS NULL OR array_length(part.\"{detail}\") = 0) "
                          "AND TextTable.language_COLUMN = ?;",
            "action_required": "SELECT count(DISTINCT part.\"H-ID\") FROM part "
                               "INNER JOIN witness ON True "
                               "INNER JOIN UNNEST(witness.\"observed_on_pages H-ID\") AS w ON w.unnest = part.\"H-ID\" "
                               "INNER JOIN TextTable ON TextTable.\"H-ID\" = witness.\"is_manifestation_of H-ID\" "
                               "WHERE part.review_status = 'Action required' AND TextTable.language_COLUMN = ?;"
        },
    "document": {
            "safe_sql_name": "DocumentTable",
            "is_corpus_data": True,
            "len_query": "SELECT COUNT(DISTINCT DocumentTable.\"H-ID\") FROM DocumentTable "
                         "INNER JOIN part ON part.\"is_inscribed_on H-ID\" = DocumentTable.\"H-ID\" "
                         "INNER JOIN witness ON True "
                         "INNER JOIN UNNEST(witness.\"observed_on_pages H-ID\") AS w ON w.unnest = part.\"H-ID\" "
                         "INNER JOIN TextTable ON TextTable.\"H-ID\" = witness.\"is_manifestation_of H-ID\" "
                         "WHERE TextTable.language_COLUMN = ?;",
            "scalar_query": "SELECT count (DISTINCT DocumentTable.\"H-ID\") FROM DocumentTable "
                            "INNER JOIN part ON part.\"is_inscribed_on H-ID\" = DocumentTable.\"H-ID\" "
                            "INNER JOIN witness ON True "
                            "INNER JOIN UNNEST(witness.\"observed_on_pages H-ID\") AS w ON w.unnest = part.\"H-ID\" "
                            "INNER JOIN TextTable ON TextTable.\"H-ID\" = witness.\"is_manifestation_of H-ID\" "
                            "WHERE DocumentTable.\"{detail}\" IS NULL AND TextTable.language_COLUMN = ?;",
            "list_query": "SELECT count (DISTINCT DocumentTable.\"H-ID\") FROM DocumentTable "
                          "INNER JOIN part ON part.\"is_inscribed_on H-ID\" = DocumentTable.\"H-ID\" "
                          "INNER JOIN witness ON True "
                          "INNER JOIN UNNEST(witness.\"observed_on_pages H-ID\") AS w ON w.unnest = part.\"H-ID\" "
                          "INNER JOIN TextTable ON TextTable.\"H-ID\" = witness.\"is_manifestation_of H-ID\" "
                          "WHERE (DocumentTable.\"{detail}\" IS NULL OR array_length(DocumentTable.\"{detail}\") = 0) "
                          "AND TextTable.language_COLUMN = ?;",
            "action_required": "SELECT count(DISTINCT DocumentTable.\"H-ID\") FROM DocumentTable "
                               "INNER JOIN part ON part.\"is_inscribed_on H-ID\" = DocumentTable.\"H-ID\" "
                               "INNER JOIN witness ON True "
                               "INNER JOIN UNNEST(witness.\"observed_on_pages H-ID\") AS w ON w.unnest = part.\"H-ID\" "
                               "INNER JOIN TextTable ON TextTable.\"H-ID\" = witness.\"is_manifestation_of H-ID\" "
                               "WHERE DocumentTable.review_status = 'Action required' "
                               "AND TextTable.language_COLUMN = ?;"
        },
    "digitization": {
            "safe_sql_name": "Digitization",
            "is_corpus_data": True,
            "len_query": "SELECT COUNT(DISTINCT digitization.\"H-ID\") FROM digitization "
                         "INNER JOIN DocumentTable ON True "
                         "INNER JOIN UNNEST(digitization.\"digitization_of H-ID\") "
                         "AS d ON d.unnest = DocumentTable.\"H-ID\" "
                         "INNER JOIN part ON part.\"is_inscribed_on H-ID\" = DocumentTable.\"H-ID\" "
                         "INNER JOIN witness ON True "
                         "INNER JOIN UNNEST(witness.\"observed_on_pages H-ID\") AS w ON w.unnest = part.\"H-ID\" "
                         "INNER JOIN TextTable ON TextTable.\"H-ID\" = witness.\"is_manifestation_of H-ID\" "
                         "WHERE TextTable.language_COLUMN = ?;",
            "scalar_query": "SELECT count (DISTINCT digitization.\"H-ID\") FROM digitization "
                            "INNER JOIN DocumentTable ON True "
                            "INNER JOIN UNNEST(digitization.\"digitization_of H-ID\") "
                            "AS d ON d.unnest = DocumentTable.\"H-ID\" "
                            "INNER JOIN part ON part.\"is_inscribed_on H-ID\" = DocumentTable.\"H-ID\" "
                            "INNER JOIN witness ON True "
                            "INNER JOIN UNNEST(witness.\"observed_on_pages H-ID\") AS w ON w.unnest = part.\"H-ID\" "
                            "INNER JOIN TextTable ON TextTable.\"H-ID\" = witness.\"is_manifestation_of H-ID\" "
                            "WHERE digitization.\"{detail}\" IS NULL AND TextTable.language_COLUMN = ?;",
            "list_query": "SELECT count (DISTINCT digitization.\"H-ID\") FROM digitization "
                           "INNER JOIN DocumentTable ON True "
                           "INNER JOIN UNNEST(digitization.\"digitization_of H-ID\") "
                           "AS d ON d.unnest = DocumentTable.\"H-ID\" "
                           "INNER JOIN part ON part.\"is_inscribed_on H-ID\" = DocumentTable.\"H-ID\" "
                           "INNER JOIN witness ON True "
                           "INNER JOIN UNNEST(witness.\"observed_on_pages H-ID\") AS w ON w.unnest = part.\"H-ID\" "
                           "INNER JOIN TextTable ON TextTable.\"H-ID\" = witness.\"is_manifestation_of H-ID\" "
                           "WHERE (digitization.\"{detail}\" IS NULL OR array_length(digitization.\"{detail}\") = 0) "
                          "AND TextTable.language_COLUMN = ?;",
            "action_required": None
        },
    "physDesc": {
            "safe_sql_name": "PhysDesc",
            "is_corpus_data": True,
            "len_query": "SELECT COUNT(DISTINCT physDesc.\"H-ID\") FROM physDesc "
                         "INNER JOIN part ON part.\"physical_description H-ID\" = physDesc.\"H-ID\" "
                         "INNER JOIN witness ON True "
                         "INNER JOIN UNNEST(witness.\"observed_on_pages H-ID\") AS w ON w.unnest = part.\"H-ID\" "
                         "INNER JOIN TextTable ON TextTable.\"H-ID\" = witness.\"is_manifestation_of H-ID\" "
                         "WHERE TextTable.language_COLUMN = ?;",
            "scalar_query": "SELECT count (DISTINCT PhysDesc.\"H-ID\") FROM PhysDesc "
                            "INNER JOIN part ON part.\"physical_description H-ID\" = PhysDesc.\"H-ID\" "
                            "INNER JOIN witness ON True "
                            "INNER JOIN UNNEST(witness.\"observed_on_pages H-ID\") AS w ON w.unnest = part.\"H-ID\" "
                            "INNER JOIN TextTable ON TextTable.\"H-ID\" = witness.\"is_manifestation_of H-ID\" "
                            "WHERE physDesc.\"{detail}\" IS NULL AND TextTable.language_COLUMN = ?;",
            "list_query": "SELECT count (DISTINCT PhysDesc.\"H-ID\") FROM PhysDesc "
                          "INNER JOIN part ON part.\"physical_description H-ID\" = PhysDesc.\"H-ID\" "
                          "INNER JOIN witness ON True "
                          "INNER JOIN UNNEST(witness.\"observed_on_pages H-ID\") AS w ON w.unnest = part.\"H-ID\" "
                          "INNER JOIN TextTable ON TextTable.\"H-ID\" = witness.\"is_manifestation_of H-ID\" "
                          "WHERE (physDesc.\"{detail}\" IS NULL OR array_length(physDesc.\"{detail}\") = 0) "
                          "AND TextTable.language_COLUMN = ?;",
            "action_required": "SELECT count(DISTINCT PhysDesc.\"H-ID\") FROM PhysDesc "
                               "INNER JOIN part ON part.\"physical_description H-ID\" = PhysDesc.\"H-ID\" "
                               "INNER JOIN witness ON True "
                               "INNER JOIN UNNEST(witness.\"observed_on_pages H-ID\") AS w ON w.unnest = part.\"H-ID\" "
                               "INNER JOIN TextTable ON TextTable.\"H-ID\" = witness.\"is_manifestation_of H-ID\" "
                               "WHERE PhysDesc.review_status = 'Action required' AND TextTable.language_COLUMN = ?;"
        },
    "stemma": {
            "safe_sql_name": "Stemma",
            "is_corpus_data": True,
            "len_query": "SELECT count(DISTINCT stemma.\"H-ID\") FROM stemma "
                         "INNER JOIN TextTable ON True "
                         "INNER JOIN UNNEST(TextTable.\"in_stemma H-ID\") AS s ON s.unnest = stemma.\"H-ID\" "
                         "WHERE TextTable.language_COLUMN = ?;",
            "scalar_query": "SELECT count(DISTINCT stemma.\"H-ID\") FROM stemma "
                            "INNER JOIN TextTable ON True "
                            "INNER JOIN UNNEST(TextTable.\"in_stemma H-ID\") AS s ON s.unnest = stemma.\"H-ID\" "
                            "WHERE stemma.\"{detail}\" IS NULL AND TextTable.language_COLUMN = ?;",
            "list_query": "SELECT count(DISTINCT stemma.\"H-ID\") FROM stemma "
                          "INNER JOIN TextTable ON True "
                          "INNER JOIN UNNEST(TextTable.\"in_stemma H-ID\") AS s ON s.unnest = stemma.\"H-ID\" "
                          "WHERE (stemma.\"{detail}\" IS NULL OR array_length(stemma.\"{detail}\") = 0) "
                          "AND TextTable.language_COLUMN = ?;",
            "action_required": None
        },
    "scripta": {
            "safe_sql_name": "Scripta",
            "is_corpus_data": True,
            "len_query": "SELECT count(DISTINCT scripta.\"H-ID\") FROM scripta "
                         "WHERE scripta.language_COLUMN = ?;",
            "scalar_query": "SELECT count(DISTINCT scripta.\"H-ID\") FROM scripta "
                            "WHERE scripta.\"{detail}\" IS NULL AND scripta.language_COLUMN = ?;",
            "list_query": "SELECT count(DISTINCT scripta.\"H-ID\") FROM scripta "
                          "WHERE (scripta.\"{detail}\" IS NULL OR array_length(scripta.\"{detail}\") = 0) "
                          "AND scripta.language_COLUMN = ?;",
            "action_required": "SELECT count(DISTINCT scripta.\"H-ID\") FROM scripta "
                               "WHERE scripta.review_status = 'Action required' AND scripta.language_COLUMN = ?;"
    },
    "images": {
        "safe_sql_name": "Images",
        "is_corpus_data": False,
        "is_action_required": False
    },
    "story": {
        "safe_sql_name": "Story",
        "is_corpus_data": False,
        "is_action_required": True
    },
    "storyverse": {
        "safe_sql_name": "Storyverse",
        "is_corpus_data": False,
        "is_action_required": True
    },
    "genre": {
        "safe_sql_name": "Genre",
        "is_corpus_data": False,
        "is_action_required": True
    },
    "repository": {
        "safe_sql_name": "Repository",
        "is_corpus_data": False,
        "is_action_required": False
    },
    "footnote": {
        "safe_sql_name": "Footnote",
        "is_corpus_data": False,
        "is_action_required": False
    },
    "person": {
        "safe_sql_name": "Person",
        "is_corpus_data": False,
        "is_action_required": False
    },
    "organisation": {
        "safe_sql_name": "Organisation",
        "is_corpus_data": False,
        "is_action_required": False
    },
    "place": {
        "safe_sql_name": "Place",
        "is_corpus_data": False,
        "is_action_required": False
    },
    "book": {
        "safe_sql_name": "Book",
        "is_corpus_data": False,
        "is_action_required": False
    },
    "thesis": {
        "safe_sql_name": "Thesis",
        "is_corpus_data": False,
        "is_action_required": False
    },
    "heurist journal volume": {
        "safe_sql_name": "HeuristJournalVolume",
        "is_corpus_data": False,
        "is_action_required": False
    },
    "journal": {
        "safe_sql_name": "Journal",
        "is_corpus_data": False,
        "is_action_required": False
    },
    "journal article": {
        "safe_sql_name": "JournalArticle",
        "is_corpus_data": False,
        "is_action_required": False
    },
    "publication series": {
        "safe_sql_name": "PublicationSeries",
        "is_corpus_data": False,
        "is_action_required": False
    },
    "non-corpus tables": {
        "len_query": "SELECT count(DISTINCT {table}.\"H-ID\") FROM {table}",
        "scalar_query": "SELECT count(DISTINCT {table}.\"H-ID\") FROM {table} "
                        "WHERE {table}.\"{detail}\" IS NULL",
        "list_query": "SELECT count(DISTINCT {table}.\"H-ID\") FROM {table} "
                      "WHERE ({table}.\"{detail}\" IS NULL OR array_length({table}.\"{detail}\") = 0)",
        "action_required": "SELECT count(DISTINCT {table}.\"H-ID\") FROM {table} "
                           "WHERE {table}.review_status = 'Action required'"

    }

}