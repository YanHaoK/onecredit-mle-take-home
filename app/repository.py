"""Version-aware writes of a company and its derived embedding."""

from dataclasses import astuple
from datetime import datetime
import logging

from psycopg import AsyncConnection

from app.config import MODEL_NAME
from app.events import CompanyEvent, StrikeOff

log = logging.getLogger(__name__)

READ_VERSION = """
    SELECT registration_number, updated_at, company_name, nature_of_business,
           industry, state, is_active
    FROM companies
    WHERE registration_number = %s
"""

UPSERT_COMPANY = """
    INSERT INTO companies (
        registration_number, updated_at, company_name, nature_of_business,
        industry, state, industry_key, source_message_id
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (registration_number) DO UPDATE SET
        is_active = TRUE,
        updated_at = EXCLUDED.updated_at,
        company_name = EXCLUDED.company_name,
        nature_of_business = EXCLUDED.nature_of_business,
        industry = EXCLUDED.industry,
        industry_key = EXCLUDED.industry_key,
        state = EXCLUDED.state,
        source_message_id = EXCLUDED.source_message_id,
        ingested_at = now()
    WHERE companies.updated_at < EXCLUDED.updated_at
    RETURNING registration_number
"""

UPSERT_EMBEDDING = """
    INSERT INTO company_embeddings
        (registration_number, embedding, model_name, source_updated_at)
    VALUES (%s, %s::real[]::vector, %s, %s)
    ON CONFLICT (registration_number) DO UPDATE SET
        embedding = EXCLUDED.embedding,
        model_name = EXCLUDED.model_name,
        source_updated_at = EXCLUDED.source_updated_at
"""

STRIKE_OFF_COMPANY = """
    INSERT INTO companies (registration_number, updated_at, source_message_id, is_active)
    VALUES (%s, %s, %s, FALSE)
    ON CONFLICT (registration_number) DO UPDATE SET
        is_active = FALSE,
        updated_at = EXCLUDED.updated_at,
        source_message_id = EXCLUDED.source_message_id,
        ingested_at = now()
    WHERE companies.updated_at < EXCLUDED.updated_at
    RETURNING registration_number
"""


async def connect(database_url: str) -> AsyncConnection:
    return await AsyncConnection.connect(
        database_url,
        autocommit=True,
        connect_timeout=5,
        options="-c statement_timeout=5000",
    )


async def existing_outcome(
    connection: AsyncConnection, event: CompanyEvent | StrikeOff, message_id: str | None
) -> str | None:
    cursor = await connection.execute(READ_VERSION, (event.registration_number,))
    current = await cursor.fetchone()
    if current is None:
        return None
    updated_at: datetime = current[1]
    if updated_at > event.updated_at:
        return "stale"
    if updated_at == event.updated_at:
        if isinstance(event, StrikeOff):
            duplicate = not current[-1]
        else:
            duplicate = current == (*astuple(event), True)
        if not duplicate:
            log.warning(
                "version_conflict company=%s updated_at=%s message_id=%s",
                event.registration_number, event.updated_at, message_id,
            )
            return "conflict"
        return "duplicate"
    return None


async def store_event(
    connection: AsyncConnection,
    event: CompanyEvent | StrikeOff,
    message_id: str | None,
    vector: list[float] | None,
) -> str:
    # The conditional write handles races after the consumer's read precheck.
    async with connection.transaction():
        if isinstance(event, StrikeOff):
            cursor = await connection.execute(
                STRIKE_OFF_COMPANY, (event.registration_number, event.updated_at, message_id)
            )
        else:
            cursor = await connection.execute(
                UPSERT_COMPANY, (*astuple(event), event.industry_key, message_id)
            )
        if await cursor.fetchone() is None:
            return await existing_outcome(connection, event, message_id) or "skipped"

        if isinstance(event, StrikeOff) or vector is None:
            await connection.execute(
                "DELETE FROM company_embeddings WHERE registration_number = %s",
                (event.registration_number,),
            )
        else:
            await connection.execute(
                UPSERT_EMBEDDING,
                (event.registration_number, vector, MODEL_NAME, event.updated_at),
            )
    return "applied"
