# OneCredit recommendation service

## Architecture

The supplied producer publishes registry events to RabbitMQ. A worker validates each delivery, computes an embedding
when the description is usable, and commits the current company and its vector to PostgreSQL. FastAPI reads committed
rows and ranks them with pgvector. Separate processes keep CPU inference and queue recovery off the request path.
PostgreSQL holds both the source record and its derived vector, so updates need one transaction and recommendation
queries need one consistent database snapshot.

## Ingestion and correctness

- The worker trims and uppercases registration numbers for identity. It preserves display fields and stores a separate
  trimmed, case-folded industry key for comparisons. Null and whitespace-only descriptions are valid. The latest
  company remains stored, and the transaction removes any previous vector.
- A timezone-aware `updatedAt` is the source version. The worker checks the current version before inference to
  skip known replays. PostgreSQL repeats the ordering check atomically in a conditional upsert. Only the transaction
  that accepts a version changes its vector. A deferred foreign key enforces that the vector references the matching
  source version. Inference runs outside the write transaction.
- The first committed payload wins when timestamps tie. Identical events are duplicates. Conflicting equal-timestamp
  payloads are logged. Delivery order can determine the winner in that case.
- Manual acknowledgment follows a successful commit or a successful read that identifies a stale or duplicate
  delivery. A crash before commit leaves the message available for redelivery. A crash after commit and before ACK
  also permits redelivery, which the version check treats as a no-op.

## Data quality in the supplied feed

The audit replayed 2,518 deliveries in file order using the worker's validation and latest-version rules.
Company counts below refer to the 2,000 latest accepted records.

| Finding | How the service handles it |
|---|---|
| Seven descriptions repeat a phrase 60 times, including the oil-palm description for `SYN-000300`. | Preserve the registry text in PostgreSQL. For embeddings and recommendation cards, collapse a multiword phrase to one copy when at least three exact copies cover the entire description. Compare case-sensitive words across whitespace. Keep additional details and partial repetitions. The embedding identifier records this preprocessing rule. |
| 24 descriptions are null and 15 are empty. Industry is missing for 102 companies and state for 49. | Keep the company. A missing description clears its vector. A known industry provides fallback cards. Missing state stays null. Four companies lack both description and industry, leaving them with no way to receive or appear in recommendations. |
| 49 valid deliveries had registration-number formatting variants. In delivery order, 63 valid events duplicated the current version and 66 were stale. | Trim and uppercase the ID and condition writes on a newer source timestamp. Companies with different IDs stay separate, including 63 companies sharing 31 names. The 1,961 usable descriptions reduce to 169 distinct cleaned texts. |
| 89 deliveries failed validation. | Quarantine the original delivery with its reason. The first reported failures were 51 timestamps, 10 registration numbers, nine data objects, five invalid JSON bodies, six null envelopes, and eight unsupported struck-off events. |

Three open findings affect interpretation of the data:

- **Company status.** Eight `company-struck-off` events are newer than the companies' last accepted upserts.
  All eight already have usable descriptions. Their prior embeddings and recommendation eligibility survive the
  quarantined status event. Confirm portal eligibility with the registry before implementing status handling.
- **Timestamp formats.** Thirty of the 51 rejected timestamps look like Unix milliseconds. Under that
  interpretation, 29 would advance the stored timestamp. All 30 carry the same name, description, industry
  and state as the retained record. The worker requires timezone-aware ISO timestamps until the unit is confirmed.
- **Location.** State capitalization varies for 313 records. The API preserves these spellings. Some descriptions
  mention a city in a different state from the state field. Registered address and operating location can differ,
  so the feed alone does not justify a correction.

## Model and ranking

The worker runs the MIT-licensed `intfloat/multilingual-e5-small` ONNX export on CPU, pinned to revision
`614241f622f53c4eeff9890bdc4f31cfecc418b3`. Each input is the cleaned and trimmed business description with the E5
`query: ` prefix. The worker truncates to 512 tokens, mean-pools using the attention mask, and L2-normalizes the
384-dimensional vector. It stores the revision and preprocessing identifier with each embedding. The same pipeline
handles English and Malay. Both languages passed an inference smoke test on Python 3.14. Recommendation quality
across languages remains unmeasured. [Model card](https://huggingface.co/intfloat/multilingual-e5-small)

PostgreSQL computes exact cosine similarity for all eligible vectors before applying the limit. It excludes the
source company and uses only current vectors from the configured model. A same-industry match adds the configurable
`INDUSTRY_BONUS` (default `0.05`). Industry is a soft signal because categories are broad and closely related
businesses can span sectors. Embedding the description alone keeps industry from contributing twice. Name and state
are display fields. The API returns cosine similarity and the adjusted ranking score separately. The adjusted score
can exceed 1.

Semantic cards sort by ranking score descending, cosine similarity descending, then registration number ascending.
Any remaining slots are filled from the same known industry in registration-number order, skipping cards already
selected. Fallback also works when the source company has no eligible vector. Both scores are null on fallback cards.
A company with neither a usable vector nor a known industry stays stored but may have no recommendations and may
never appear as a candidate. At roughly 2,000 companies, exact scoring keeps the weighted ordering straightforward.
The implementation uses no approximate vector index.

## Delivery and service operation

The consumer declares the registry-owned `company-events` queue as durable with its original arguments and
prefetches one delivery. Transaction conflicts retry up to three attempts with backoff. Connection failures
trigger reconnection with capped exponential backoff. Exhausted transaction retries, inference failures and
unexpected SQL errors stop the worker with its delivery pending for restart after repair.
The local database connection and statement timeouts are five seconds.

Malformed and unsupported events go to the durable `company-events.dlq` queue with the original body,
message ID, and reason. The worker ACKs the original only after RabbitMQ confirms quarantine publication. A crash between
quarantine confirmation and ACK can create duplicate quarantine messages. Recovery from quarantine requires manual
inspection and replay. Compose named volumes retain broker and database state across container recreation.
`just down` removes both volumes.

`GET /healthz` reports process liveness. `GET /companies/SYN-001777/similar?limit=5` returns the canonical query
ID, ranking method, applied bonus, and recommendation cards. Each card supplies registration number, name, industry,
state, description, match type, and both scores. `ranking=cosine` sets the bonus to zero for the request. For example,
an unknown registration returns HTTP 404 with this body.

```json
{"detail":{"code":"COMPANY_NOT_FOUND","message":"Company not found"}}
```

Invalid limits or ranking values return HTTP 422. Temporary database failures return HTTP 503 with
`SERVICE_UNAVAILABLE`. A known company without any matches returns HTTP 200 with an empty array.
The API pool holds up to five connections, bounds waiting requests, checks connections on checkout, allows two
seconds for pool acquisition, and sets a five-second statement timeout. Logs report consumer outcomes and retries,
API query duration, and error classes. Client error bodies omit credentials and internal errors.

## Verification

- The complete `uv run --locked python -m unittest discover -s tests -v` suite ran with `ONECREDIT_TEST_DATABASE_URL` pointed
  at an isolated PostgreSQL 16 instance with pgvector 0.8.7. All 13 tests passed. The tests exercised validation,
  ranking and fallback, concurrent version writes, rollback, and a simulated ACK failure after a real commit.
- Final verification installed the locked dependencies in a fresh Python environment and started Compose with
  empty volumes. A two-message fixture published with the supplied producer went through the worker and the running
  HTTP API. Two identical descriptions produced a cosine score of 1 within numerical tolerance. The smoke run
  reused previously downloaded model files. The RabbitMQ health check runs as the broker user so it can share
  the Erlang cookie on first startup.
- The supplied producer fed 2,518 deliveries into a fresh RabbitMQ queue. The worker stored 2,000 companies and
  1,961 embeddings with matching source versions, and quarantined 89 malformed deliveries. The second publish
  preserved the company and embedding rows and the live API response, and quarantined the same 89 messages again.
  The default five cards for `SYN-001777` were `SYN-000036`, `SYN-000040`, `SYN-000157`, `SYN-000172`,
  and `SYN-000284`.
- A newer null description removed the vector and the API returned same-industry cards with null scores. An older
  delivery left that state intact. A live PostgreSQL outage returned HTTP 503, and the API and worker recovered
  after restart. The same worker reconnected after a RabbitMQ restart. A queued event and a quarantine message
  survived RabbitMQ container recreation, and a restarted worker processed the queued event.

## At 10× volume

Measure queue lag, inference throughput, and API p50/p95 latency with representative data first. If inference
limits throughput, batch encodes or add worker processes while retaining the conditional write and bounded database
connections. Size the API pool against database capacity and track pool wait time, retries, lag, and quarantine
growth. If exact query latency becomes a problem, evaluate HNSW candidate retrieval against exact weighted top-K
recall, with particular attention to candidates that the industry bonus lifts into the top results. For a model
change, build a versioned replacement set of vectors and switch the active version once coverage and quality have
been verified.

The implementation leaves relevance labeling, bonus tuning, latency benchmarking, and the optional company
metadata endpoint for future work. No benchmark or ranking-quality result is claimed.

## AI disclosure

OpenAI Codex assisted with implementation planning, code generation, code review, automated tests, local test
execution, and documentation. The verification above records checks run in the development workspace.
