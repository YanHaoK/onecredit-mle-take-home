BEGIN;

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS companies (
    registration_number TEXT PRIMARY KEY,
    company_name TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    nature_of_business TEXT,
    industry TEXT,
    industry_key TEXT,
    state TEXT,
    updated_at TIMESTAMPTZ NOT NULL,
    source_message_id TEXT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (registration_number, updated_at),
    -- A strike-off can arrive before the first company profile.
    CHECK (NOT is_active OR company_name IS NOT NULL)
);

CREATE TABLE IF NOT EXISTS company_embeddings (
    registration_number TEXT PRIMARY KEY,
    embedding VECTOR(384) NOT NULL,
    model_name TEXT NOT NULL,
    source_updated_at TIMESTAMPTZ NOT NULL,
    -- A transaction can update the company before replacing or removing its vector.
    FOREIGN KEY (registration_number, source_updated_at)
        REFERENCES companies (registration_number, updated_at)
        ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED
);

COMMIT;
