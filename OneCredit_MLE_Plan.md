# OneCredit MLE Take-Home — Implementation Plan & Codex Astra Handoff

**Status (22 November 2026):** Core implementation merged. The merged design records full-feed ingestion, replay,
and restart verification. The follow-up adds the company metadata endpoint, regression coverage for clearing a
description, an opt-in real-model smoke test, and a reproducible latency benchmark. All 19 tests passed with
PostgreSQL/pgvector and cached E5 weights. The warm weighted route measured 10.92 ms p50 and 19.25 ms p95
over 200 sequential requests. The checklist below preserves the original scope. See `DESIGN.md` for the
implemented failure policy and verification evidence. Human relevance labeling, model comparison, and
industry-bonus tuning remain deferred after choosing the latency deep dive.
**Prepared:** 20 November 2026  
**Assessment:** `mle-v2026.11` — [OneCreditMY/mle-take-home](https://github.com/OneCreditMY/mle-take-home)  
**Primary deliverables:** working RabbitMQ consumer, PostgreSQL + pgvector store, company-recommendation FastAPI service, automated tests, `README.md`, and `DESIGN.md`.

> **Instructions to Codex Astra:** This is an implementation specification, not an invitation to overengineer. Read the upstream repository's `README.md`, `producer/publish.py`, `docker-compose.yml`, `Justfile`, and sample feed *before changing code*. The official assessment is the authority where this plan and the repository disagree. Preserve the producer's behavior and the existing `just setup/up/publish/down` workflows. Implement the **P0** requirements first, run the supplied feed end to end, write honest documentation, then consider **P1** enhancements if time permits. Keep the solution understandable enough for the candidate to explain every line and trade-off in a technical interview. Do not claim tests or benchmarks ran unless they actually ran. Report changed files, commands run, tests/results, unresolved issues, and deviations from this plan.

---

## 1. Goals, constraints, and acceptance criteria

### Objective

Build the backend for a portal's **Similar companies** panel: ingest changing company records from RabbitMQ; persist the latest valid company version plus its business-description embedding in PostgreSQL/pgvector; serve other companies ranked by business similarity, with enough metadata for cards.

### Verified upstream facts

- Assessment version: `mle-v2026.11`; target effort **4–6 hours**, maximum 6 hours. There is an **optional** deep-dive hour *within* the six hours.
- Provided `data/feed.jsonl`: **2,518 messages** concerning **2,000 synthetic Malaysian companies**, including duplicates, updates, and out-of-order deliveries.
- Provided producer sends raw JSON lines to a pre-existing **durable** queue named `company-events`; `aio-pika` uses persistent AMQP messages, publisher confirms, and a SHA-256-of-body `message_id`.
- The registry team owns that queue and declares it **durable with no extra arguments**. **Do not redeclare it with a DLX, TTL, quorum setting, or other incompatible arguments.**
- Scaffold: PostgreSQL 16 with pgvector (`pgvector/pgvector:0.8.7-pg16`), RabbitMQ (`rabbitmq:3.13.7-management`), `uv`, Python **3.14**, `just`, Docker Compose.
- Provided commands: `just setup`, `just up`, `just publish`, `just down` (`down` deliberately removes volumes); database defaults to `postgresql://postgres:postgres@localhost:5432/companies`.
- Mandatory: CPU-runnable **open-source** embedding model, pgvector similarity search **inside PostgreSQL**, consumer, recommendation API, own startup README and `DESIGN.md`. **No paid embedding APIs.**
- Evaluation emphasis: correctness, recovery, tests, documented assumptions, code readability. AI tool use must be disclosed in `DESIGN.md`.

### P0 acceptance criteria — must work

- [ ] Reproducible setup from a clean checkout, with accurate one-command or minimal-command README instructions.
- [ ] Consumer receives events from the supplied `company-events` queue without changing the producer or queue contract.
- [ ] Latest source record wins by `updatedAt`; duplicate and older events do not overwrite newer state, even during concurrent processing/replay.
- [ ] All valid companies are stored, **including those with missing descriptions**. No stale or wrong-version embedding accompanies an updated row.
- [ ] Appropriate ACK/retry/dead-letter behavior; no ACK before a successful DB commit; producer replay and consumer restart are safe.
- [ ] CPU embeddings of usable `natureOfBusiness` descriptions, stored in `pgvector`, with model provenance.
- [ ] API returns a ranked list of *other* companies with company ID, name, industry, state, and transparent scores.
- [ ] Exclude the query company and duplicates. Return deterministically ordered semantic matches first, then labeled same-industry fallback up to the requested limit.
- [ ] Automated tests cover stale/duplicate updates, null descriptions, transaction/ACK boundary, SQL ranking, and API errors.
- [ ] `README.md` and `DESIGN.md` explain usage, choices, known limitations, and a path to 10× scale.

### Scope guardrails

**Do:** Docker Compose, straightforward Python modules, explicit SQL/transactions, bounded retries, simple logging.  
**Do not:** Kubernetes, AWS RDS/Multi-AZ, Redis, Celery, distributed tracing infrastructure, elaborate microservices, mandatory LLM calls, ANN index without evidence, or unnecessary abstraction layers. These are future-scale discussion topics, not required implementation.

---

## 2. System architecture and ownership

```text
                         [provided producer: just publish]
                                      |
                         durable company-events queue
                                      |
                                  RabbitMQ
                                      |
                      [consumer: prefetch=1; manual ACK]
                           |             |
                  validate/version   CPU embedding model
                           |             |
                           +-------------+
                                      |
                       one PostgreSQL transaction
                          |                       |
                companies (source data)   company_embeddings (derived)
                          +------------+----------+
                                       |
                                PostgreSQL + pgvector
                                       ^
                                       |
                             FastAPI recommendation API
                                       |
                               portal / curl / tests
```

- **Producer / registry:** external, provided, not edited; authoritative event envelope and version timestamp.
- **RabbitMQ:** delivery/ACK/replay. Queue ownership stays with registry team.
- **Consumer:** validates events, embeds descriptions, conditionally commits *both* company row and matching embedding, ACKs afterwards. A single worker with `prefetch_count=1` is fine initially.
- **PostgreSQL:** source-of-truth latest company state; embeddings are replaceable derived data.
- **API:** reads only committed, mutually consistent company+embedding versions; never embeds during a request.
- **Model:** one embedding model and dimension per deployed index; avoid comparing vectors generated by different models even if their dimensions match.

**Read consistency:** an embedding is computed *outside* a long DB transaction; the transaction then atomically writes the new company version and embedding. Until commit, the API sees the prior committed pair, not half an update. If embedding fails for a non-null description, do not partially publish the new version with an old embedding.

---

## 3. Event contract, validation, and normalization

Example received JSON body:

```json
{
  "type": "company-upserted",
  "registrationNumber": "SYN-001777",
  "updatedAt": "2026-08-14T09:00:00+08:00",
  "data": {
    "companyName": "Global Maju Packaging Sdn. Bhd.",
    "natureOfBusiness": "Production of rubber gloves for export",
    "state": "Negeri Sembilan",
    "industry": "Manufacturing"
  }
}
```

**Validation decisions:**

1. Require a JSON object, supported event type, nonblank registration number, parseable *timezone-aware* `updatedAt`, and valid `data` object. Validate expected types; reject malformed payloads without killing the worker.
2. Parse `updatedAt` as a timezone-aware timestamp and store as `TIMESTAMPTZ` (UTC-equivalent comparison). This source timestamp determines company version, **not** delivery order or AMQP `message_id`.
3. Preserve original company name, business description, industry, and state for display; keep nulls. Normalize industry for *comparison only*: trim whitespace + case-fold (and treat empty strings as unknown). Normalize state only when needed for display/filtering; don't accidentally modify source meaning.
4. If `natureOfBusiness` is null, blank, or only whitespace, **accept and persist the newer event**, and atomically remove any prior embedding. Do not reject a valid source update because the derived vector is unavailable.
5. Names are metadata, not the embedding input. State is metadata/possible future filter, **not** a geographic boost. Industry is the *only* extra ranking signal in P0.
6. `message_id` helps correlate logs/diagnose redelivery, but cannot by itself decide which version wins. Producer uses SHA-256 of the raw line: separate source events and duplicate messages must be handled at the entity/version level.
7. Equal `updatedAt`: treat exactly identical versions as idempotent no-ops. For different payloads at the same timestamp, the simple **first-committed version wins** rule is acceptable only if documented and a conflict is logged; it is *not* order-independent. **P1:** a documented deterministic tie-break (e.g., canonical payload hash) if such collisions appear in the feed. Do not silently claim total ordering when the source supplies none.

**Bad event policy:** permanently malformed/unsupported events should go to a consumer-managed `company-events.dlq` (or similarly named durable quarantine queue) with reason and original body. Publish with confirmation, **then** ACK the original delivery; if quarantine publication fails, do not ACK. Do not add dead-letter arguments to the source queue. If DLQ implementation cannot fit the deadline, document the limitation rather than discarding poison messages silently.

---

## 4. PostgreSQL schema and integrity invariants

Use the `vector` extension. Dimension is tied to the **actual chosen embedding model**; `384` below is the proposed 384-dimensional baseline and must be checked before migration.

```sql
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS companies (
    registration_number TEXT PRIMARY KEY,
    company_name TEXT NOT NULL,
    nature_of_business TEXT,
    industry TEXT,
    state TEXT,
    updated_at TIMESTAMPTZ NOT NULL,
    source_message_id TEXT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS company_embeddings (
    registration_number TEXT PRIMARY KEY
        REFERENCES companies(registration_number) ON DELETE CASCADE,
    embedding VECTOR(384) NOT NULL,
    model_name TEXT NOT NULL,
    source_updated_at TIMESTAMPTZ NOT NULL
);
```

**Required invariants:**

- Exactly one latest row in `companies` per `registration_number`.
- An embedding exists **only** when the latest description is usable and successfully embedded.
- If an embedding exists, `company_embeddings.source_updated_at = companies.updated_at`.
- API query and candidate vectors have the *same exact* `model_name`/model version, dimension, and preprocessing protocol.
- A source record and its matching embedding are committed or rolled back together. Never expose a new description with an old vector.
- A newer null-description update removes the old embedding in the *same transaction*.
- Never compare different embedding-model vector spaces, even if both happen to be 384-dimensional.

### Correct version-aware transaction

**Recommended implementation sequence:**

1. Validate event. Optionally read current source version as an optimization to skip redundant embedding work; **never rely on this precheck for correctness**.
2. If the new description is usable, calculate its vector **before** acquiring a write transaction. Validate finite, nonzero vector of the expected dimension; apply the model's required input formatting and normalization consistently.
3. Open a short PostgreSQL transaction.
4. Conditional insert/upsert of `companies`, using `WHERE companies.updated_at < EXCLUDED.updated_at` in `ON CONFLICT DO UPDATE` and `RETURNING` to learn whether **this transaction accepted the new version**. This compares atomically against concurrent updates; no Python read-then-write race.
5. **Only if accepted:** upsert matching `company_embeddings` (same source timestamp, model identifier), or `DELETE` embedding for the newer null/blank description.
6. If no row was returned, this is a duplicate or stale event: don't touch company or embedding. Optionally log an equal-timestamp payload conflict.
7. Commit. **Only after successful commit**, ACK the RabbitMQ delivery.
8. On SQL/embedding/transient failures, roll back/no ACK and retry according to the failure policy. If commit outcome is uncertain, treat redelivery as normal: timestamp-aware idempotency must make it safe.

**Concurrency insight:** conditional upsert and embedding mutation must occur in one transaction. A racing older event will fail the version condition after the newer one commits and must not overwrite its vector. Don't maintain separate company and embedding write transactions; don't keep the DB transaction open during model inference.

**Index decision:** primary-key btree indexes are sufficient for identities; no HNSW/IVFFlat index in P0. For ~2,000 embeddings, exact full-candidate cosine scoring supports the weighted ranker's correctness and is easy to debug.

---

## 5. RabbitMQ delivery, recovery, and poison messages

| Case | Required behavior |
|---|---|
| New valid event | Embed (if usable), conditional atomic DB commit, ACK afterwards. |
| Duplicate/redelivery | Conditional version check skips write; ACK after successful check/transaction. |
| Stale/out-of-order event | Ignore newer-state overwrite, no embedding change, ACK. |
| DB unavailable during processing | Roll back; remain unacked during **bounded** local retry/backoff; don't hammer DB. |
| Extended DB outage | Stop/close/reconnect consumer with exponential backoff; unacked delivery becomes redeliverable. Avoid indefinite unacked holds and acknowledgement timeout. |
| Consumer crash before commit | Transaction rolls back; unacked message redelivered. |
| Consumer crash after commit but before ACK | Message redelivered; version check makes replay idempotent. |
| Unsupported/malformed event | Publish to durable quarantine queue with confirmation, then ACK original; log structured reason. |
| Embedding/transient model failure | Bounded retry with backoff; no partial new-version commit. After repeated failures, quarantine with provenance if the failure is persistent; never silently lose it. |
| Broker connection loss | Use `aio_pika.connect_robust`/reconnection loop; re-establish consuming and channel state. |
| Broker restart | Durable queue + persistent messages + persistent broker data volume (see Compose) to support restart/recreate testing. |

Practical choices:

- `auto_ack=False`; manual `ACK` only after success; `basic_qos(prefetch_count=1)` for predictable processing initially.
- Avoid `NACK(requeue=True)` in a tight loop when PostgreSQL is down. Back off locally; stop/reconnect on sustained outage. Cap local waiting to avoid RabbitMQ's consumer-ack timeout.
- Exponential backoff with a cap and modest jitter; use explicit timeouts for DB connection/queries. A simple single-worker approach is sufficient.
- RabbitMQ durability is **not the same** as Postgres durability. Add named volumes for PostgreSQL and RabbitMQ in `docker-compose.yml` so recreated containers preserve data; explain that `just down` uses `down -v` and deliberately resets them.
- Do not introduce automatic purging, queue declaration changes, or manual deletion of unacknowledged messages.
- Consider reconnect and DLQ publisher-confirm failures in tests. Logs include message ID, registration number, source timestamp, outcome, retries, and error class.

---

## 6. Embedding model and bilingual data

### Settled representation decision

**Embed only `natureOfBusiness`.** Do **not** concatenate `industry`, `companyName`, or `state` to the embedding text. Rationale: business description is the direct semantic signal; adding industry to embedding text *and* to the scoring bonus risks double-counting. Names/state can mislead relevance.

### Model selection — evaluation-guided, with a pragmatic default

- Assessment requires a free, open-source, CPU-runnable model. No paid API dependency in ingestion, API or tests.
- The feed contains **both English and Malay** text. `BAAI/bge-small-en-v1.5` (384d) is the provided English-focused example, but English-only performance is *not* enough to select a production candidate.
- Prefer a compact **multilingual model that genuinely supports Malay**, if installation, CPU latency, cross-language behavior, and quality are acceptable. A candidate to verify is `intfloat/multilingual-e5-small` (typically 384 dimensions; confirm model card, license, intended input prefixes, compatibility). Do **not** assume multilingual automatically means good Malay support.
- Evaluate three concepts if time allows: (A) English-focused model on original text; (B) Malay-to-English translation + English embeddings; (C) direct multilingual embedding. Compare English-English, Malay-Malay, and **English-Malay** query/candidate pairs.
- Translation adds latency, another model/service, and possible mistranslations; implement it only if a measured quality benefit justifies complexity. **For P0 choose one feasible CPU model, use it consistently, and document why; don't build a three-pipeline framework.**
- Preserve model preprocessing requirements (e.g., E5's published text-prefix convention, if chosen). Every company vector and query vector must be generated consistently for this symmetric company-to-company matching task. Verify library compatibility with the scaffold's Python 3.14 before committing to a package; prefer a straightforward supported CPU inference path.
- Use consistent batch or sequential inference; cache/download model once where possible. Handle cold-start latency honestly. Do not silently substitute random/hash embeddings unless clearly disclosed as a temporary dev-only fallback.
- If switching models after records have been embedded, **re-embed all records** before serving comparisons, or reject incompatible comparisons. Store `model_name` and source version explicitly.

**Missing descriptions.** Persist the latest metadata and remove any prior vector atomically. A company without a description remains eligible for same-industry recommendations as both the query and a candidate.

---

## 7. Similarity retrieval, industry reranking, and API

### Chosen ranking strategy

Primary: **weighted semantic + industry bonus**, with pure cosine similarity retained as a baseline/configuration for comparison.

```text
cosine_similarity = 1 - cosine_distance
industry_match = 1 only if both industries are known and equal after normalization
ranking_score = cosine_similarity + INDUSTRY_BONUS * industry_match
```

- `INDUSTRY_BONUS` is configurable (e.g., tentative `0.03`–`0.05`, **not a measured optimum**). Choose a defensible small default, compare with `0.0`, and document its effect. Disable if it degrades reviewed recommendations.
- **Soft bonus, not hard industry filtering.** Cross-industry companies can have closely related business activities; broad/missing/mislabeled industry categories are imperfect.
- **Score all eligible candidates before `LIMIT`**, then order by final score. Retrieving only 5 cosine neighbors and reranking those 5 can exclude valid final top-5 candidates. Top-50 is a heuristic; exact all-candidate scoring is simpler/correct for ~2k rows.
- Exclude query company. Filter to matching `model_name` and source-version-consistent embeddings. Null industry receives zero bonus; null-description candidates have no vector.
- Stable order: `ranking_score DESC`, then `cosine_similarity DESC`, then `registration_number ASC`. A bonus-adjusted score may exceed 1; it is **not a probability**. Return semantic and ranking scores separately.

Illustrative SQL structure (adapt names/dimensions to actual code; use bound parameters, not string interpolation):

```sql
SELECT
    c.registration_number,
    c.company_name,
    c.nature_of_business,
    c.industry,
    c.state,
    1.0 - (e.embedding <=> :query_embedding) AS cosine_similarity,
    (1.0 - (e.embedding <=> :query_embedding))
      + CASE
          WHEN NULLIF(LOWER(BTRIM(c.industry)), '') = :query_normalized_industry
               AND :query_normalized_industry IS NOT NULL
          THEN :industry_bonus
          ELSE 0.0
        END AS ranking_score
FROM company_embeddings e
JOIN companies c ON c.registration_number = e.registration_number
WHERE c.registration_number <> :query_registration_number
  AND e.model_name = :query_model_name
  AND e.source_updated_at = c.updated_at
ORDER BY ranking_score DESC, cosine_similarity DESC, c.registration_number ASC
LIMIT :limit;
```

Fetch the query company with an optional eligible vector. Run the semantic query in PostgreSQL when that vector exists. Fill only remaining slots with companies sharing the same known, normalized industry, including candidates without vectors. Exclude the query ID and already selected IDs. Order fallback cards by registration number ascending. If the query lacks a vector, all slots are available for fallback. Skip fallback when industry is unknown.

### Proposed HTTP contract (assessment leaves routes open)

- `GET /healthz` — simple liveness; separate readiness endpoint if genuinely useful.
- `GET /companies/{registration_number}` — P1 but valuable to prove null-description records weren't lost (metadata only).
- `GET /companies/{registration_number}/similar?limit=5` — P0; optionally `ranking=weighted|cosine` for explicit baseline testing, default weighted. Validate `limit`, e.g. `1..20`.

**Example response** (illustrative, actual IDs/scores are not claimed results):

```json
{
  "query_company": "SYN-001777",
  "ranking_method": "weighted_semantic_industry",
  "industry_bonus": 0.05,
  "recommendations": [
    {
      "registration_number": "SYN-000123",
      "company_name": "Example Manufacturing Sdn. Bhd.",
      "industry": "Manufacturing",
      "state": "Selangor",
      "nature_of_business": "Production of medical latex gloves",
      "match_type": "semantic",
      "cosine_similarity": 0.88,
      "ranking_score": 0.93
    }
  ]
}
```

Each card includes `match_type`. For `same_industry` cards, return JSON null for both `cosine_similarity` and `ranking_score`. The portal can label these cards “Other companies in {industry}”.

**Error policy.** Return structured JSON errors and keep stack traces and credentials out of responses.

| Condition | Proposed HTTP status | Machine code |
|---|---:|---|
| Unknown registration number | `404` | `COMPANY_NOT_FOUND` |
| Existing company with no description | `200` with same-industry cards if available, otherwise empty recommendations | — |
| Invalid `limit` or other query param | `422` | `INVALID_REQUEST` / FastAPI validation shape |
| DB/service temporarily unavailable | `503` | `SERVICE_UNAVAILABLE` |
| Existing company with no eligible candidates in either group | `200` with empty recommendations | — |

Connection pools should be bounded and opened/closed with application lifecycle; acquisition/query timeouts; PostgreSQL `statement_timeout` or equivalent; ensure cleanup even on cancellation/error. API should serve previously committed data during background ingestion. API database errors do not silently return a misleading successful empty list.

### Observability

- Structured logs with event, level, company ID where appropriate, `message_id`/request ID, source timestamp, latency, error code/retry count. No secrets or raw stack traces in client responses.
- Basic counters/timing if trivial: processed/applied/skipped/stale/poison events, consumer retries, request success/errors, API latency, DB exceptions. No Prometheus/Grafana/Datadog requirement.

---

## 8. Startup, resilience, and developer experience

- Preserve supplied `Justfile` semantics. Existing `just up` starts Postgres/RabbitMQ; add application services and health dependencies to Compose **or** document clean `uv run ...` startup commands. Prefer a clearly reproducible workflow and validate from zero.
- If Compose services are added, use `depends_on: condition: service_healthy` for initial ordering and `restart: unless-stopped` (or suitable restart policy); these **do not replace** application-level retries/reconnections.
- Add persistent named volumes for Postgres/RabbitMQ. `just down` remains a deliberately destructive reset; state it clearly.
- DB initialization must run automatically or via one documented command: extension/schema creation before consumer/route queries. A plain idempotent SQL migration is enough; Alembic optional only if already justified.
- Model weight/cache download should not happen on every request/message; ensure an offline/first-run explanation and reasonable cache directory if containerized.
- Use timeouts, `try/finally`/context managers, controlled shutdown, and graceful handling of SIGTERM where practical.
- Pin dependencies in `uv.lock`; use `.env.example` for optional config, never commit secrets. The scaffold's development credentials are local defaults only.
- Follow Python 3.14 compatibility requirements; avoid unsupported native binary dependencies. If a model library forces a version change, document and minimize divergence from the template.

### Suggested small project layout

```text
.
├── producer/                 # provided; leave unchanged
├── data/feed.jsonl           # provided; leave unchanged
├── app/
│   ├── config.py             # settings
│   ├── db.py                 # connection and schema initialization
│   ├── models.py             # event validation and API schemas
│   ├── embeddings.py         # model load/encode/provenance
│   ├── consumer.py           # receive/retry/ack/DLQ
│   ├── repository.py         # conditional upsert + exact ranking SQL
│   └── api.py                # FastAPI routes
├── sql/001_init.sql          # extension/schema
├── tests/                    # unit + focused integration tests
├── Dockerfile                # only if packaging consumer/API in Compose
├── docker-compose.yml        # extend supplied services safely
├── pyproject.toml / uv.lock
├── README.md
└── DESIGN.md
```

This is an example organization, **not a requirement**; prefer fewer modules if clearer. Share DB and embedding logic rather than duplicating it in API and consumer.

---

## 9. Testing plan (prioritized)

### P0 unit tests — fast, repeatable

- [ ] Parse valid English/Malay event, timezone-aware timestamp, null industry/description, case/whitespace normalization.
- [ ] Reject malformed JSON, unexpected types, missing required fields, missing/naive date; route to poison handling.
- [ ] Ranking formula including industry match/mismatch/null, ties, self-exclusion, configurable zero bonus.
- [ ] Embedding input is *only* `natureOfBusiness`; missing/whitespace input has no vector; dimensions/finite values validated.
- [ ] API schema/error shape, `limit` bounds, response metadata.

### P0 integration tests — use real Postgres/RabbitMQ where practical

- [ ] Fresh DB + feed ingestion: unique company count matches the feed's final entity set (expected **2,000**, if all events are valid); consumer drains queue and returns records.
- [ ] Replay full feed: no duplicate company rows; same final values/embedding versions; no surprising reorder effects.
- [ ] Inject an **older** event after a newer one: latest source version remains unchanged.
- [ ] Concurrent/interleaved updates for same company: conditional SQL guards state and vector together.
- [ ] Newer non-null description changes vector and metadata in one commit.
- [ ] Newer null/blank description keeps company metadata but **deletes old embedding**.
- [ ] Simulate failure **after DB commit, before ACK**: redelivered event must be idempotent and ACKed safely.
- [ ] Simulate DB failure before commit: transaction rolls back, message remains recoverable, no misleading ACK.
- [ ] Malformed event: durable quarantine (if implemented), later valid events still process.
- [ ] API excludes the source company, sorts semantic cards by weighted score, respects model provenance, and returns expected errors.
- [ ] Exercise full and partially filled semantic lists, a description cleared by an update, and a query missing both description and industry. Verify fallback eligibility in both directions, null scores, stable ordering, no duplicates, and the total limit.
- [ ] Stop/restart consumer and RabbitMQ, then confirm processing resumes; persistence depends on Compose volumes and durable/persistent messages.

Use deterministic small synthetic fixtures rather than the full model for most tests. Mock embedding generation in unit tests, but run at least one **real-model smoke test** to validate model download/inference, dimension, Malay/English inputs, pgvector storage, and API retrieval. Never label a mock-only test as end-to-end model verification.

### Explicit manual demonstration

```text
1. Start Postgres and RabbitMQ; initialize schema.
2. Start consumer, then execute `just publish`.
3. Observe processed/applied/stale counts and queue drain.
4. Query known example company (e.g. SYN-001777) for top 5.
5. Replay `just publish`; verify stable company count/output.
6. Interrupt consumer during processing, restart; verify eventual completion.
7. Send a controlled newer update, then an older update; verify latest wins.
8. Test a company whose latest natureOfBusiness is null.
9. Capture actual commands and outcomes in README/DESIGN.
```

---

## 10. Evaluation and optional deep dive (P1, not before core works)

**Objective:** determine whether weighted semantic+industry ranking actually improves *business relevance* over pure cosine; do not present hypothetical numbers as empirical measurements.

1. Choose ~10–20 diverse query companies, covering English, Malay, ambiguous descriptions, cross-language pairs, broad/missing industry, and different business types.
2. Generate candidates from cosine baseline and weighted ranker; union results, deduplicate and **blind** their origin/rank when labeling.
3. Define the relevance rubric *before* looking at results: `0` unrelated, `1` broadly related, `2` strongly related, `3` near-identical business activity. Be clear whether related supply-chain firms count as relevant; don't automatically award relevance because industry matches.
4. LLM-assisted draft labels are fine for exploratory work, but **human-review** them. LLM-generated judgments are not objective ground truth; they may favor matching industry or familiar wording. No paid LLM API in the submitted application.
5. Split by **query company** into validation and held-out test sets. Tune `INDUSTRY_BONUS` on validation queries only, freeze it, then report test-set `NDCG@5` and qualitative examples. Small test sizes mean low confidence; be explicit.
6. Evaluate English-English, Malay-Malay, and English-Malay examples for any model comparison. A model that wins on English alone may lose badly across languages.
7. Optional: inspect top-five examples, false positives, missing-description coverage, simple API p50/p95 latency for warm model/DB and whether industry bonus changes top-five membership.

**Time constraint:** one optional deep dive consumes at most one hour of the allotted six. Choose **either** small recommendation-quality analysis **or** focused recovery/latency analysis; don't promise comprehensive benchmarking or a multi-model training pipeline.

---

## 11. Suggested execution order and time budget

| Phase | Priority | Target effort | Definition of done |
|---|---|---:|---|
| Repo inspection & scaffolding | P0 | ~20 min | Understand producer, queue, feed; choose model; create settings/schema. |
| Robust ingestion | P0 | ~90 min | Consumer, conditional atomic upsert, vectors, ACK/retry; valid full-feed processing. |
| Recommendation API | P0 | ~60 min | pgvector exact ranking, industry bonus, error contract, response cards. |
| Integration and reliability tests | P0 | ~70 min | Replay, stale update, null update, crash-after-commit behavior, restart smoke. |
| Docs + final verification | P0 | ~40 min | README/DESIGN with actual commands/results/limits and AI disclosure. |
| Optional chosen deep dive / contingency | P1 | ~60 min | Small quality evaluation **or** recovery/latency evidence; only if P0 done. |

Time allocations are a flexible **six-hour ceiling**, not an estimate of actual execution time. Stop and document unfinished work rather than hide it. Prefer a fully working simple solution to an elaborate partial architecture.

### Codex implementation checkpoints

- **Checkpoint A:** Before coding, report conflicts/ambiguities between this plan and upstream README, selected embedding model + dimension/requirements, and proposed minimal file layout.
- **Checkpoint B:** After schema and consumer, show the transaction/retry/ACK boundary, the null update path, and evidence of replay correctness.
- **Checkpoint C:** After API, show the exact SQL ensuring all-candidate weighted ranking, self-exclusion, and model isolation.
- **Checkpoint D:** After tests, report actual pass/fail commands and full-feed count, demonstrate error cases, and identify incomplete requirements.
- **Checkpoint E:** Final `DESIGN.md` documents only implemented decisions as implemented, and distinguishes future improvements; no fabricated measurements.

**Important for candidate ownership:** avoid inscrutable code, generic abstract factories, unnecessary decorators, or invented operational infrastructure. Use simple Python, explicit function boundaries, type hints, tests, and comments on the *why* for ordering/transaction decisions. The candidate will be asked to explain the solution line by line.

---

## 12. Required `DESIGN.md` contents

- **Architecture & data flow:** why separate consumer/API, why Postgres+pgvector and two-table schema.
- **Correctness:** at-least-once delivery, manual ACK after commit, timestamp-based ordering, race-free conditional upsert, duplicate handling, crash window, equal-timestamp policy.
- **Data-quality semantics:** null descriptions, nullable industry, Malay+English, source-preserving normalization, coverage trade-off versus the portal's “no lost companies” expectation.
- **Model:** exact name/version, CPU inference implementation, dimensionality, input preprocessing, Malay handling, compatibility/provenance, reason for selected model.
- **Ranking:** cosine baseline, configurable soft industry bonus, why not industry-first or industry-in-embedding, why exact all-candidate scoring/no ANN, self-exclusion, tie-breaks, limitations.
- **Reliability:** bounded retry/backoff, unhealthy dependencies, broker/database restarts, queue ownership, DLQ limitations, persistent volumes.
- **API contract:** routes, sample payloads, errors, timeouts, latency and observability choices.
- **Verification:** actual tests executed, representative inputs/outputs, known gaps; small/manual evaluation only if actually conducted.
- **10× scale:** profile first; batch embeddings, more consumers with safe DB conditional writes, appropriate pool sizing, latency benchmarks, candidate retrieval + ANN (e.g., HNSW) only after weighted ranking recall is preserved, model re-embedding/migration strategy, monitoring/alerts, backpressure.
- **AI disclosure:** name AI tools used (including Codex Astra if used), and specify whether used for architecture discussion, code generation, review, tests, docs. State that code/results were validated by the candidate as far as true.
- **Trade-offs / what's left:** honestly list any P1 deferred or failure behavior not implemented.

---

## 13. Decision register: agreed vs deferred

| Area | Decision | Status |
|---|---|---|
| System | RabbitMQ → single consumer → Postgres/pgvector → FastAPI | Agreed |
| Queue | Durable `company-events`, no extra declaration args, manual ACK | Verified upstream + agreed |
| Version | Source `updatedAt`; atomic conditional newer-only upsert | Agreed |
| Duplicates | Replay-safe via source version, not message ID alone | Agreed |
| Embedding consistency | Company + derived vector committed together; source version metadata | Agreed |
| Schema | Separate company and embedding tables; model provenance | Agreed |
| Embedding text | `natureOfBusiness` only | Agreed |
| Ranking | Cosine + configurable same-industry soft bonus; cosine baseline | Agreed |
| Extra ranking fields | No state/name boost in P0 | Agreed |
| Candidate selection | Exact cosine against all eligible vectors, bonus before top-K | Agreed |
| ANN index | None initially (dataset ~2k) | Agreed |
| Missing description | Persist metadata and atomically remove the obsolete vector | Agreed |
| Bilingual model | Benchmark English/Malay/cross-language; select feasible CPU model | Evaluation principle agreed; **final model selection open** |
| Evaluation | LLM may help label; human-validate; held-out queries; NDCG@5 | Agreed as P1 |
| Reliability | Startup health checks + app reconnect/backoff; no tight requeue loop | Agreed |
| Infrastructure | Compose, not K8s or AWS for assessment | Agreed |
| Equal timestamp, different payload | First committed wins + log; deterministic tie-break optional | Documented implementation assumption |
| Poison events | Consumer-managed quarantine queue, without editing source queue args | Proposed pragmatic implementation detail |
| Recommendation priority | Return semantic cards first, then fill unused slots with labeled same-industry cards | Agreed P0 |
| Industry weight | Small configurable bonus, compare to zero | **Value to validate**, not invented as empirical optimum |

## 14. Sources and handoff notes

Official source of truth:

- [Assessment README](https://github.com/OneCreditMY/mle-take-home/blob/main/README.md)
- [Provided producer](https://github.com/OneCreditMY/mle-take-home/blob/main/producer/publish.py)
- [Provided Compose stack](https://github.com/OneCreditMY/mle-take-home/blob/main/docker-compose.yml)
- [Provided Justfile](https://github.com/OneCreditMY/mle-take-home/blob/main/Justfile)
- [Synthetic event feed](https://github.com/OneCreditMY/mle-take-home/blob/main/data/feed.jsonl)

**This file is a handoff plan, not executed code or a benchmark report.** The actual implementation must inspect upstream files and validate runtime/library compatibility. User-approved preferences from the architecture discussion have been preserved; explicit ambiguities and honest limitations are identified so an implementation agent does not silently invent policy.
