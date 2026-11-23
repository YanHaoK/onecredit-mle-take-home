"""Local service connection settings, with the scaffold's port overrides."""

import math
import os
from dataclasses import dataclass

MODEL_REPO = "Qdrant/paraphrase-multilingual-MiniLM-L12-v2-onnx-Q"
MODEL_REVISION = "faf4aa4225822f3bc6376869cb1164e8e3feedd0"
MODEL_NAME = f"{MODEL_REPO}@{MODEL_REVISION}:onnx-128-mean-l2"


@dataclass(frozen=True)
class Settings:
    database_url: str
    rabbitmq_url: str

    @classmethod
    def from_env(cls) -> "Settings":
        postgres_port = os.getenv("POSTGRES_PORT", "5432")
        rabbitmq_port = os.getenv("RABBITMQ_PORT", "5672")
        return cls(
            database_url=os.getenv(
                "DATABASE_URL",
                f"postgresql://postgres:postgres@localhost:{postgres_port}/companies",
            ),
            rabbitmq_url=os.getenv(
                "RABBITMQ_URL", f"amqp://guest:guest@localhost:{rabbitmq_port}/"
            ),
        )


def read_industry_bonus() -> float:
    bonus = float(os.getenv("INDUSTRY_BONUS", "0.05"))
    if not math.isfinite(bonus) or not 0 <= bonus <= 1:
        raise ValueError("INDUSTRY_BONUS must be a finite number between 0 and 1")
    return bonus
