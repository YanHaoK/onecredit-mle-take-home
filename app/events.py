"""Validate registry events while preserving the values used for display."""

import json
from dataclasses import dataclass
from datetime import datetime, timezone


class InvalidEvent(ValueError):
    """A delivery that cannot be processed as a company upsert."""


def read_text(
    values: dict, key: str, *, required: bool = False
) -> str | None:
    value = values.get(key)
    if value is None and not required:
        return None
    if not isinstance(value, str) or "\x00" in value:
        raise InvalidEvent(f"{key} must be valid text")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise InvalidEvent(f"{key} must be valid UTF-8 text") from exc
    if required and not value.strip():
        raise InvalidEvent(f"{key} must not be blank")
    return value


def registration_key(value: str) -> str:
    """Resolve case and surrounding whitespace variants to the same company."""
    return value.strip().upper()


def display_description(value: str | None) -> str | None:
    """Collapse a whole multiword phrase copied at least three times."""
    if not value:
        return value
    words = value.split()
    for width in range(1, len(words) // 3 + 1):
        if len(words) % width or words[width] != words[0]:
            continue
        phrase = words[:width]
        if phrase * (len(words) // width) == words:
            return " ".join(phrase) if width > 1 else value
    return value


@dataclass(frozen=True)
class CompanyEvent:
    registration_number: str
    updated_at: datetime
    company_name: str
    nature_of_business: str | None
    industry: str | None
    state: str | None

    @property
    def industry_key(self) -> str | None:
        return (self.industry or "").strip().casefold() or None

    @property
    def embedding_text(self) -> str | None:
        return (display_description(self.nature_of_business) or "").strip() or None


def parse_event(body: bytes) -> CompanyEvent:
    try:
        payload = json.loads(body.decode("utf-8"))
    except (ValueError, RecursionError) as exc:
        raise InvalidEvent("invalid UTF-8 JSON body") from exc

    if not isinstance(payload, dict) or payload.get("type") != "company-upserted":
        raise InvalidEvent("expected a company-upserted event")
    data = payload.get("data")
    if not isinstance(data, dict):
        raise InvalidEvent("data must be an object")

    raw_timestamp = payload.get("updatedAt")
    if not isinstance(raw_timestamp, str):
        raise InvalidEvent("updatedAt must be a timezone-aware ISO timestamp")
    try:
        updated_at = datetime.fromisoformat(raw_timestamp)
        if updated_at.utcoffset() is None:
            raise ValueError("missing timezone")
        updated_at = updated_at.astimezone(timezone.utc)
    except (ValueError, OverflowError) as exc:
        raise InvalidEvent("updatedAt must be a timezone-aware ISO timestamp") from exc

    registration_number = read_text(payload, "registrationNumber", required=True)
    return CompanyEvent(
        registration_number=registration_key(registration_number),
        updated_at=updated_at,
        company_name=read_text(data, "companyName", required=True),
        nature_of_business=read_text(data, "natureOfBusiness"),
        industry=read_text(data, "industry"),
        state=read_text(data, "state"),
    )
