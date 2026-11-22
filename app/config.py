"""Local service connection settings, with the scaffold's port overrides."""

import math
import os
from dataclasses import dataclass

MODEL_REPO = "intfloat/multilingual-e5-small"
MODEL_REVISION = "614241f622f53c4eeff9890bdc4f31cfecc418b3"
MODEL_NAME = f"{MODEL_REPO}@{MODEL_REVISION}:onnx-query-mean-l2:collapse-repeats-v1"


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
