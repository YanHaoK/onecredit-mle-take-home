"""HTTP endpoints for the portal's company recommendations."""

from contextlib import asynccontextmanager
import logging
from time import perf_counter
from typing import Annotated, Literal

from fastapi import FastAPI, HTTPException, Query, Request
from psycopg import Error as DatabaseError, OperationalError
from psycopg.errors import QueryCanceled
from psycopg_pool import ConnectionPool
from pydantic import BaseModel

from app.config import Settings, read_industry_bonus
from app.events import registration_key
from app.recommendations import find_similar

log = logging.getLogger("uvicorn.error")


class Recommendation(BaseModel):
    registration_number: str
    company_name: str
    industry: str | None
    state: str | None
    nature_of_business: str | None
    match_type: Literal["semantic", "same_industry"]
    cosine_similarity: float | None
    ranking_score: float | None


class SimilarCompanies(BaseModel):
    query_company: str
    ranking_method: Literal["weighted_semantic_industry", "cosine"]
    industry_bonus: float
    recommendations: list[Recommendation]


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = Settings.from_env()
    industry_bonus = read_industry_bonus()
    pool = ConnectionPool(
        settings.database_url,
        min_size=1,
        max_size=5,
        max_waiting=20,
        timeout=2,
        check=ConnectionPool.check_connection,
        kwargs={
            "autocommit": True,
            "connect_timeout": 5,
            "options": "-c statement_timeout=5000",
        },
        open=False,
    )
    with pool:
        app.state.pool = pool
        app.state.industry_bonus = industry_bonus
        yield


app = FastAPI(title="OneCredit Company Recommendations", lifespan=lifespan)


@app.get("/healthz")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/companies/{registration_number}/similar", response_model=SimilarCompanies)
def similar(
    registration_number: str,
    request: Request,
    limit: Annotated[int, Query(ge=1, le=20)] = 5,
    ranking: Literal["weighted", "cosine"] = "weighted",
) -> SimilarCompanies:
    started = perf_counter()
    registration = registration_key(registration_number)
    if not registration or "\x00" in registration:
        raise HTTPException(422, detail={"code": "INVALID_REQUEST", "message": "Invalid registration number"})
    bonus = request.app.state.industry_bonus if ranking == "weighted" else 0.0
    try:
        with request.app.state.pool.connection() as connection:
            cards = find_similar(connection, registration, limit, bonus)
    except (OperationalError, QueryCanceled) as exc:
        log.warning("recommendation_unavailable company=%r error=%s", registration, type(exc).__name__)
        raise HTTPException(
            503, detail={"code": "SERVICE_UNAVAILABLE", "message": "Recommendations temporarily unavailable"}
        ) from exc
    except DatabaseError as exc:
        log.error("recommendation_query_failed company=%r error=%s", registration, type(exc).__name__)
        raise HTTPException(
            500, detail={"code": "INTERNAL_ERROR", "message": "Unable to retrieve recommendations"}
        ) from exc
    if cards is None:
        raise HTTPException(404, detail={"code": "COMPANY_NOT_FOUND", "message": "Company not found"})
    log.info(
        "recommendations company=%r ranking=%s count=%d elapsed_ms=%.1f",
        registration, ranking, len(cards), (perf_counter() - started) * 1000,
    )
    return SimilarCompanies(
        query_company=registration,
        ranking_method="weighted_semantic_industry" if ranking == "weighted" else "cosine",
        industry_bonus=bonus,
        recommendations=[Recommendation(**card) for card in cards],
    )
