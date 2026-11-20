"""Rank recommendations in PostgreSQL using one committed statement snapshot."""

from psycopg import Connection
from psycopg.rows import dict_row

from app.config import MODEL_NAME

RECOMMENDATIONS = """
    WITH query_company AS (
        SELECT c.registration_number, c.industry_key, e.embedding
        FROM companies c
        LEFT JOIN company_embeddings e
          ON e.registration_number = c.registration_number
         AND e.source_updated_at = c.updated_at
         AND e.model_name = %(model)s
        WHERE c.registration_number = %(registration)s
    ), candidates AS (
        SELECT c.registration_number, c.company_name, c.industry, c.state,
               c.nature_of_business,
               1.0 - (e.embedding <=> q.embedding) AS cosine_similarity,
               CASE WHEN c.industry_key = q.industry_key
                    THEN %(bonus)s::double precision ELSE 0.0 END AS bonus
        FROM query_company q
        JOIN company_embeddings e
          ON e.registration_number <> q.registration_number
         AND e.model_name = %(model)s
        JOIN companies c
          ON c.registration_number = e.registration_number
         AND c.updated_at = e.source_updated_at
        WHERE q.embedding IS NOT NULL
    ), semantic AS (
        SELECT registration_number, company_name, industry, state, nature_of_business,
               'semantic' AS match_type, cosine_similarity,
               cosine_similarity + bonus AS ranking_score, 0 AS tier
        FROM candidates
        ORDER BY ranking_score DESC, cosine_similarity DESC, registration_number
        LIMIT %(limit)s
    ), fallback AS (
        SELECT c.registration_number, c.company_name, c.industry, c.state,
               c.nature_of_business, 'same_industry' AS match_type,
               NULL::double precision AS cosine_similarity,
               NULL::double precision AS ranking_score, 1 AS tier
        FROM query_company q
        JOIN companies c ON c.industry_key = q.industry_key
        WHERE c.registration_number <> q.registration_number
          AND NOT EXISTS (
              SELECT 1 FROM semantic s WHERE s.registration_number = c.registration_number
          )
        ORDER BY c.registration_number
        LIMIT (%(limit)s - (SELECT count(*) FROM semantic))
    ), cards AS (
        SELECT * FROM semantic
        UNION ALL
        SELECT * FROM fallback
    )
    SELECT cards.registration_number, cards.company_name, cards.industry, cards.state,
           cards.nature_of_business, cards.match_type, cards.cosine_similarity,
           cards.ranking_score
    FROM query_company
    LEFT JOIN cards ON true
    ORDER BY cards.tier, cards.ranking_score DESC, cards.cosine_similarity DESC,
             cards.registration_number
"""


def find_similar(
    connection: Connection, registration: str, limit: int, bonus: float
) -> list[dict] | None:
    with connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute(
            RECOMMENDATIONS,
            {"registration": registration, "model": MODEL_NAME, "limit": limit, "bonus": bonus},
        )
        rows = cursor.fetchall()
    if not rows:
        return None
    # The left join distinguishes an existing company with zero cards from a missing company.
    if rows[0]["registration_number"] is None:
        return []
    return rows
