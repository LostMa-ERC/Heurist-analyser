LOSTMA_TABLES = {
    "text": {
        "safe_sql_name": "TextTable",
        "language_filter": "WHERE language_COLUMN = ?;",
        "type": "My record types"
    },
    "witness": {
            "safe_sql_name": "Witness",
            "language_filter": """WHERE EXISTS (
                                    SELECT 1
                                    FROM TextTable
                                    WHERE TextTable.\"H-ID\" = Witness.\"is_manifestation_of H-ID\"
                                      AND TextTable.language_COLUMN = ?
                                );""",
            "type": "My record types"
        },
    "part": {
            "safe_sql_name": "Part",
            "language_filter": """WHERE EXISTS (
                                    SELECT 1
                                    FROM Witness
                                    CROSS JOIN UNNEST(Witness.\"observed_on_pages H-ID\") AS p(part_id)
                                    JOIN TextTable ON Witness.\"is_manifestation_of H-ID\" = TextTable.\"H-ID\"
                                    WHERE p.part_id = Part.\"H-ID\"
                                      AND TextTable.language_COLUMN = ?
                                )""",
            "type": "My record types"
        },
    "document": {
            "safe_sql_name": "DocumentTable",
            "language_filter": """WHERE EXISTS (
                                    SELECT 1
                                    FROM Witness
                                    CROSS JOIN UNNEST(Witness."observed_on_pages H-ID") AS p(part_id)
                                    JOIN Part ON p.part_id = Part.\"H-ID\"
                                    JOIN TextTable ON Witness.\"is_manifestation_of H-ID\" = TextTable.\"H-ID\"
                                    WHERE Part.\"is_inscribed_on H-ID\" = DocumentTable.\"H-ID\"
                                      AND TextTable.language_COLUMN = ?
                                )""",
            "type": "My record types"
        },
    "digitization": {
            "safe_sql_name": "Digitization",
            "language_filter": """WHERE EXISTS (
                                                SELECT 1
                                                FROM UNNEST(Digitization.\"digitization_of H-ID\") AS d(document_id)
                                                JOIN DocumentTable ON d.document_id = DocumentTable.\"H-ID\"
                                                JOIN Part ON Part.\"is_inscribed_on H-ID\" = DocumentTable.\"H-ID\"
                                                JOIN (
                                                    SELECT
                                                        Witness."is_manifestation_of H-ID" AS text_id,
                                                        obs.part_id
                                                    FROM Witness
                                                    CROSS JOIN UNNEST(Witness."observed_on_pages H-ID") AS obs(part_id)
                                                ) witness_parts
                                                  ON witness_parts.part_id = Part."H-ID"
                                                JOIN TextTable ON witness_parts.text_id = TextTable.\"H-ID\"
                                                WHERE TextTable.language_COLUMN = ?
                                            )""",
            "type": "My record types"
        },
    "physDesc": {
            "safe_sql_name": "PhysDesc",
            "language_filter": """WHERE EXISTS (
                                                SELECT 1
                                                FROM Witness
                                                CROSS JOIN UNNEST(Witness."observed_on_pages H-ID") AS p(part_id)
                                                JOIN Part ON p.part_id = Part.\"H-ID\"
                                                JOIN TextTable 
                                                ON Witness.\"is_manifestation_of H-ID\" = TextTable.\"H-ID\"
                                                WHERE Part.\"physical_description H-ID\" = PhysDesc.\"H-ID\"
                                                    AND TextTable.language_COLUMN = ?
                                                )""",
            "type": "My record types"
        },
    "stemma": {
            "safe_sql_name": "Stemma",
            "language_filter": """WHERE EXISTS (
                                                SELECT 1
                                                FROM TextTable
                                                CROSS JOIN UNNEST(TextTable.\"in_stemma H-ID\") AS s(stemma_id)
                                                WHERE s.stemma_id = Stemma.\"H-ID\"
                                                WHERE TextTable.language_COLUMN = ?
                                                )""",
            "type": "My record types"
        },
    "scripta": {
            "safe_sql_name": "Scripta",
            "language_filter": "WHERE language_COLUMN = ?;",
            "type": "My record types"
    },
    "story": {
        "safe_sql_name": "Story",
        "language_filter": """WHERE EXISTS (
                                    SELECT 1
                                    FROM TextTable
                                    CROSS JOIN UNNEST(TextTable."is_expression_of H-ID") AS s(story_id)
                                    WHERE s.story_id = Story.\"H-ID\"
                                      AND TextTable.language_COLUMN = ?
                                )""",
        "type": "My record types"
    },
    "storyverse": {
        "safe_sql_name": "Storyverse",
        "language_filter": """WHERE EXISTS (
                                            FROM Story
                                            CROSS JOIN UNNEST(Story."is_part_of_storyverse H-ID") AS sv(storyverse_id)
                                            JOIN (
                                                SELECT
                                                    TextTable."H-ID" AS text_id,
                                                    TextTable.language_COLUMN as language,
                                                    rel_story.story_id
                                                FROM TextTable
                                                CROSS JOIN UNNEST(TextTable.\"is_expression_of H-ID\") 
                                                    AS rel_story(story_id)                                            
                                            ) text_story
                                             ON text_story.story_id = Story."H-ID"
                                            WHERE sv.storyverse_id = Storyverse.\"H-ID\"
                                                AND language = ?
                                            )""",
        "type": "My record types"
    },
    "genre": {
        "safe_sql_name": "Genre",
        "language_filter": """WHERE EXISTS (
                                            SELECT 1
                                            FROM TextTable
                                            WHERE TextTable.\"specific_genre H-ID\" = Genre.\"H-ID\"
                                              AND TextTable.language_COLUMN = ?
                                        )""",
        "type": "My record types"
    },
    "images": {
        "safe_sql_name": "Images",
        "type": "My record types"
    },
    "repository": {
        "safe_sql_name": "Repository",
        "type": "My record types"
    },
    "footnote": {
        "safe_sql_name": "Footnote",
        "type": "My record types"
    },
    "person": {
        "safe_sql_name": "Person",
        "type": "People and organisations"
    },
    "organisation": {
        "safe_sql_name": "Organisation",
        "type": "People and organisations"
    },
    "place": {
        "safe_sql_name": "Place",
        "type": "Place, features"
    },
    "book": {
        "safe_sql_name": "Book",
        "type": "Bibliography"
    },
    "thesis": {
        "safe_sql_name": "Thesis",
        "type": "Bibliography"
    },
    "heurist journal volume": {
        "safe_sql_name": "HeuristJournalVolume",
        "type": "Bibliography"
    },
    "journal": {
        "safe_sql_name": "Journal",
        "type": "Bibliography"
    },
    "journal article": {
        "safe_sql_name": "JournalArticle",
        "type": "Bibliography"
    },
    "publication series": {
        "safe_sql_name": "PublicationSeries",
        "type": "Bibliography"
    },
    "TextTable": {"normal_name": "text"},
    "Witness": {"normal_name": "witness"},
    "Part": {"normal_name": "part"},
    "DocumentTable": {"normal_name": "document"},
    "Digitization": {"normal_name": "digitization"},
    "PhysDesc": {"normal_name": "physDesc"},
    "Stemma": {"normal_name": "stemma"},
    "Scripta": {"normal_name": "scripta"},
    "Images": {"normal_name": "images"},
    "Story": {"normal_name": "story"},
    "Storyverse": {"normal_name": "storyverse"},
    "Genre": {"normal_name": "genre"},
    "Repository": {"normal_name": "repository"},
    "Footnote": {"normal_name": "footnote"},
    "Person": {"normal_name": "person"},
    "Organisation": {"normal_name": "organisation"},
    "Place": {"normal_name": "place"},
    "Book": {"normal_name": "book"},
    "Thesis": {"normal_name": "thesis"},
    "HeuristJournalVolume": {"normal_name": "heurist journal volume"},
    "Journal": {"normal_name": "journal"},
    "JournalArticle": {"normal_name": "journal article"},
    "PublicationSeries": {"normal_name": "publication series"}
}