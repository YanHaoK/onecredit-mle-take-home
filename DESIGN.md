# OneCredit recommendation service

## Architecture

The supplied producer publishes registry events to RabbitMQ. A worker validates each delivery, computes an embedding
when the description is usable, and commits the current company and its vector to PostgreSQL. FastAPI reads committed
rows and ranks them with pgvector. Separate processes keep CPU inference and queue recovery off the request path.
PostgreSQL holds both the source record and its derived vector, so updates need one transaction and recommendation
queries need one consistent database snapshot.

## Ingestion and correctness

- **Data normalization:** Registration numbers are trimmed and uppercased to identify companies consistently. Records with missing descriptions are retained, but any existing embedding is removed to prevent stale recommendations.
- **Out-of-order updates:** The source `updatedAt` timestamp determines which version of a company is accepted. The worker checks for stale or duplicate events before generating embeddings, while PostgreSQL enforces the version check atomically to prevent older updates from overwriting newer records.
- **Data consistency:** Company records and their embeddings are updated within the same transaction, ensuring they remain consistent. Embedding inference runs outside the transaction to avoid holding database resources during computation.
- **Duplicate handling:** Events with identical timestamps and content are treated as duplicates. When timestamps match but payloads conflict, the first committed version takes precedence and the conflict is logged.


## Optional deep dive — registry data quality and late-arriving updates

Thirty deliveries had a numeric `updatedAt`. Interpreting those numbers as Unix milliseconds would advance
29 stored timestamps. All 30 had the same name, description, industry and state as the retained record.
The timestamp still matters because the worker uses it to decide which future updates to accept. Guessing
the unit could cause a genuine change to be discarded as stale. The worker therefore keeps these deliveries
in `company-events.dlq` with their original payload and rejection reason. If the registry confirms the unit,
they can be replayed through the existing version check once the parser supports milliseconds.

Of the other 21 deliveries rejected for timestamps, 14 omitted the time zone and seven omitted `updatedAt`
entirely. For example, `2026-08-14T09:06:19` in Malaysia and the same clock time in UTC are eight hours apart.
Replay requires the registry to confirm the intended zone or supply the missing source timestamp.
Using delivery time could let an older update overwrite a newer record.

The audit replayed all 2,518 deliveries in file order. The table summarizes what happened under the worker's
validation and version rules. Company counts refer to the 2,000 latest accepted records.

| Finding | How the service handles it |
|---|---|
| Seven descriptions repeat a phrase 60 times, including the oil-palm description for `SYN-000300`. | Preserve the registry text in PostgreSQL. For embeddings and recommendation cards, collapse a multiword phrase to one copy when at least three exact copies cover the entire description. Compare case-sensitive words across whitespace. Keep additional details and partial repetitions. Rebuilding from the supplied feed applies this rule to every stored vector. |
| 24 descriptions are null and 15 are empty. Industry is missing for 102 companies and state for 49. | Keep the company. A missing description clears its vector. A known industry provides fallback cards. Missing state stays null. Four companies lack both description and industry, leaving them with no way to receive or appear in recommendations. |
| 49 valid deliveries had registration-number formatting variants. In delivery order, 63 valid events duplicated the current version and 66 were stale. | Trim and uppercase the ID. Update the stored company only when the source timestamp is newer. |
| 30 names each appear under two registration numbers, and one name appears under three. Among 1,961 companies with usable descriptions there are 169 distinct cleaned texts. | Keep each registration number as a separate company, even when names or business descriptions match. |
| 81 deliveries failed validation. | Quarantine the original delivery with its reason. The first reported failures were 51 timestamps, 10 registration numbers, nine data objects, five invalid JSON bodies, and six null envelopes. |

**Company status.** A newer `company-struck-off` marks the company inactive and removes its embedding in
one transaction. The row retains its last known profile. If the strike-off arrives first, the worker stores
the registration number and source timestamp so a late, older upsert cannot reopen the company.
A strictly newer upsert reactivates it. Equal timestamps follow the existing first-committed rule.
An inactive company's page receives an empty panel, and both ranking paths exclude it from other panels.

**Open location finding.** State capitalization varies for 313 records. The API preserves these spellings.
Some descriptions mention a city in a different state from the state field. Registered address and operating
location can differ, so the feed alone does not justify a correction.

## Model and ranking

The service uses the MIT-licensed intfloat/multilingual-e5-small model with ONNX Runtime for CPU inference. I chose a multilingual model to support both English and Malay business descriptions without translation. Each description is converted into a 384-dimensional embedding, with model and preprocessing versions stored to maintain consistency.

Recommendations are ranked using exact cosine similarity in PostgreSQL, with a configurable same-industry bonus (default 0.05). Only the business description is embedded, while industry acts as an additional ranking signal. This allows companies with similar business activities to rank highly even when they belong to different industries.

When there are insufficient semantic matches, the service fills the remaining recommendations with companies from the same industry. Companies without usable descriptions can still receive industry-based recommendations. Given the dataset size of approximately 2,000 companies, I chose exact vector search over approximate indexing to keep the ranking logic simple.

## Delivery and service operation

The RabbitMQ consumer uses manual acknowledgments to avoid losing messages before processing completes. Transient failures are retried, while persistent processing failures stop the worker and leave the message unacknowledged for recovery after the issue is resolved.

Malformed or unsupported events are sent to a durable dead-letter queue (company-events.dlq) for manual review instead of being retried indefinitely. The original message is acknowledged only after RabbitMQ confirms successful publication to the DLQ.

The FastAPI service exposes GET /companies/{registration_number}/similar for recommendations and GET /healthz for liveness checks. It returns HTTP 404 for unknown companies, HTTP 200 with an empty list when no recommendations are available, and HTTP 503 for temporary database failures. Database connections and query timeouts are bounded to prevent resource exhaustion.

## Verification

- All 20 tests passed against an isolated pgvector database, including strike-off redelivery, reactivation, ranking, concurrent updates, rollback, and acknowledgment handling.

- Two passes through RabbitMQ with the pinned E5 encoder produced identical company and embedding rows. Each pass processed 2,518 deliveries and sent 81 to quarantine. PostgreSQL retained 2,000 companies, including 1,992 active companies and 1,953 current embeddings. All eight struck-off companies returned empty panels.

- **Failure recovery:** Verified that stale updates cannot overwrite newer records, missing descriptions remove existing embeddings, and the service handles PostgreSQL and RabbitMQ outages. Queued messages and quarantined events also survived broker container recreation.

## At 10× volume

With 20,000 companies, I would keep exact cosine similarity search unless API latency becomes a problem. The dataset is still relatively small, and exact search avoids the additional complexity of approximate vector indexes.

For a continuously higher incoming event rate, I would first measure consumer throughput and RabbitMQ queue growth. If embedding inference becomes the bottleneck, I would run multiple consumers to process messages in parallel. The existing timestamp-based conditional upsert protects against older updates overwriting newer ones, even when messages are processed out of order.

I would monitor CPU usage, database connections, and API latency before making further changes.

## AI disclosure

OpenAI Codex assisted with implementation planning, code generation, code review, automated tests, local test
execution, and documentation. The verification above records checks run in the development workspace.
