# MLE Take-Home: Company Recommendation System

Welcome to the **Machine Learning Engineer** take-home from the OneCredit Data & AI team. It's a simplified version of
a system we run in production, where it powers company recommendations and lead generation.

**Who it's for:** people early in their career, with 0 to 2 years of experience. Fresh graduates are welcome.

If you applied, thanks for your time. If you didn't, we hope this helps you learn something new, such as RabbitMQ or
pgvector.

Assessment version: **2026.11** (`mle-v2026.11`). What changed between versions is in [`CHANGELOG.md`](CHANGELOG.md).

## Run locally

Install Git, [Docker with Compose](https://docs.docker.com/get-started/get-docker/), [just](https://just.systems/man/en/installation.html), and [uv](https://docs.astral.sh/uv/getting-started/installation/). On Windows, run the commands in Git Bash. Start Docker. The first run needs internet access for Python 3.14, dependencies, container images, and the embedding model.

**1. Terminal 1 — clone the project.** If you already have a checkout, open it in your terminal and skip this block.

```sh
git clone https://github.com/YanHaoK/onecredit-mle-take-home.git
cd onecredit-mle-take-home
```

**2. Terminal 1 — install dependencies and start Postgres and RabbitMQ.** Wait for both services to become healthy and the prompt to return.

```sh
just setup
just up
```

**3. Terminal 2 — start the consumer.** Open a second terminal in the same project folder and leave the worker running. It downloads the embedding model on its first run.

```sh
just worker
```

**4. Back in terminal 1 — publish the sample feed.** When the producer finishes, it prints `published 2518 messages to company-events`. The worker in terminal 2 processes those messages.

```sh
just publish
```

**5. Terminal 3 — start the API.** Open a third terminal in the same project folder and leave the API running.

```sh
just api
```

**6. Try a recommendation.** Once Uvicorn starts, open [similar companies for SYN-001777](http://127.0.0.1:8000/companies/SYN-001777/similar?limit=5) in your browser. You should see JSON with a `recommendations` list. If you get a 404 while the worker loads the feed, wait a moment and refresh.

**Shut down.** Press Ctrl+C in terminals 2 and 3 to stop the worker and API. In terminal 1, choose one of the following commands.

**For a clean shutdown and reset**, run this to delete the local Postgres data and RabbitMQ queues. On your next run, repeat steps 2 through 5 to reload the sample feed.

```sh
just down
```

**If you want to keep your data**, run this instead.

```sh
just stop
```

After `just stop`, resume with `just up` in terminal 1, `just worker` in terminal 2, and `just api` in terminal 3. You can skip publishing because the data is still there.

## The scenario

When someone opens a company's page on the OneCredit portal, we want to show a **"Similar companies"** panel next to
it. Someone looking at a coffee shop chain in Selangor should see other companies that do similar things.

The registry team already publishes every company change to a RabbitMQ queue. Your job is to build the service that
powers that panel from the feed.

### What the user sees

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  OneCredit Portal                                         Search companies…  │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Sinar Teguh Bakery Sdn. Bhd.                                   SYN-001550   │
│  Food & Beverage · Selangor                                                  │
│  Operating cafes and coffee shops in the Klang Valley                        │
│                                                                              │
│  Similar companies                                                           │
│  ┌────────────────────────────────────┐  ┌────────────────────────────────┐  │
│  │ Seri Sejahtera Food Sdn. Bhd.      │  │ Everbright Pacific Food        │  │
│  │ Food & Beverage · Johor            │  │ Sdn. Bhd.                      │  │
│  │                                    │  │ Food & Beverage · Kuala Lumpur │  │
│  └────────────────────────────────────┘  └────────────────────────────────┘  │
│  ┌────────────────────────────────────┐  ┌────────────────────────────────┐  │
│  │ Gemilang Delima Food Sdn. Bhd.     │  │ Pacific Everbright Logistik    │  │
│  │ Food & Beverage · Johor            │  │ PLT                            │  │
│  │                                    │  │ Logistics · Kuala Lumpur       │  │
│  └────────────────────────────────────┘  └────────────────────────────────┘  │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

Clicking a card opens that company's page, with its own "Similar companies" panel.

### How it works for the user

```mermaid
sequenceDiagram
    actor User as Portal user
    participant Portal as OneCredit portal
    participant Service as Your service
    participant DB as Postgres + pgvector
    User->>Portal: Opens the page for SYN-001550
    Portal->>Service: Similar companies for SYN-001550
    Service->>DB: Nearest neighbours of SYN-001550
    DB-->>Service: Closest companies
    Service-->>Portal: Similar companies, most similar first
    Portal-->>User: Page with the "Similar companies" panel
    User->>Portal: Clicks Seri Sejahtera Food Sdn. Bhd.
    Portal->>Service: Similar companies for SYN-000539
```

### How the data gets there

```mermaid
flowchart LR
    P[Registry feed<br/>producer] -->|company-events| Q[(RabbitMQ)]
    Q --> C[Your consumer]
    C -->|embed descriptions| E[Embedding model]
    C --> D[(Postgres + pgvector<br/>similarity search)]
    A[Your service] --> D
    W[OneCredit portal] --> A
```

### Notes from the registry team

> The feed merges a few upstream sources and we don't validate what they send. Delivery is at-least-once, and every
> message has an AMQP `message_id`. Each event carries the time the change happened at the source in `updatedAt`. We own
> the `company-events` queue and declare it as durable with no extra arguments. We can't change the format before your
> launch, so please work with what's there.

### What the portal needs

- Recommendations that reflect the latest information we've received about each company.
- No lost companies. If a company is in the feed, users expect to see it on the portal and in other companies'
  recommendations.
- Fast enough to render while the company page loads.

## The task

1. Build a **consumer** that reads the `company-events` queue and stores each company in Postgres, with an embedding
   of its business description in pgvector.
2. Build the **service** the portal calls: given a company, return its most similar companies, with what the portal
   needs to draw each card in the mock above. Run the similarity search in Postgres with pgvector. Matching companies
   on their nature of business is enough for a first version.
3. Write a short `DESIGN.md` covering the decisions you made, the assumptions you made where the scenario was unclear,
   what you would change to handle 10x the volume, and what's left.
4. Optional, up to one hour of your six: go deeper in one area you're strong in. For example: how good the
   recommendations are, latency at a realistic scale, how you'd monitor this in production, or a design for explaining
   a recommendation with an LLM (a design only; no paid APIs). Write it up in `DESIGN.md`.

The rest is your call: the schema, the embedding model, your tests and tooling, and the API, including what the portal
gets back when something goes wrong. Tell us in your own README how to start your consumer and service, and we'll
follow it.

Where the scenario leaves something open, make a reasonable call and write it down. There isn't one right answer.

## Embeddings

Use an open-source embedding model that runs on a laptop CPU. Don't call a paid API. Some options:

- `BAAI/bge-small-en-v1.5`, a small CPU model (384 dimensions), through
  [fastembed](https://qdrant.github.io/fastembed/) or sentence-transformers. The panel in the mock above comes from it.
- A placeholder while you build, such as a hashed bag-of-words vector, as long as you say so in `DESIGN.md`.

## Local configuration

- **Ports.** Set `POSTGRES_PORT`, `RABBITMQ_PORT`, or `RABBITMQ_UI_PORT` in both terminals before startup to override the defaults. The default database URL is `postgresql://postgres:postgres@localhost:5432/companies`. The [RabbitMQ management UI](http://localhost:15672) uses `guest` for both username and password. Explicit `DATABASE_URL` and `RABBITMQ_URL` settings take precedence for the Python services that use them.
- **Model cache.** The worker reuses the downloaded multilingual E5 model. Set `HF_HOME` to choose the cache location or `HF_HUB_OFFLINE=1` to use a populated cache offline.
- **Existing databases.** Compose initializes the schema on an empty Postgres volume. For an existing database, apply [the schema](sql/001_init.sql) with `psql` before starting the worker.
- **Full reset.** To rebuild old embeddings after the description cleanup, stop the worker and API, then run `just down`. **This deletes the local database and RabbitMQ queues, including quarantined messages.** Repeat steps 2 through 5 to rebuild from the feed. Replaying alone skips previously accepted versions and quarantines malformed deliveries again.

## Recommendation API

`GET /healthz` reports process liveness. The recommendation route accepts limits from 1 to 20 and returns
semantic matches first. It adds an industry bonus of 0.05 by default. Set `INDUSTRY_BONUS` before starting
the API to adjust the weight, or pass `ranking=cosine` to use zero bonus for one request. If there are slots
left, the API fills them with same-industry matches and returns null scores for those cards.

## Run the tests

```bash
uv run --locked python -m unittest discover -s tests -v
```

Set `ONECREDIT_TEST_DATABASE_URL` to the connection string for an isolated PostgreSQL database initialized
with `sql/001_init.sql` to enable the pgvector integration tests. They insert and clean up their own fixtures
and cover ranking, concurrent version writes, rollback, and retry after a simulated acknowledgment failure.

Local verification with Python 3.14, Docker Compose, and the pinned E5 CPU model processed all 2,518 feed
deliveries, stored 2,000 companies and 1,961 current embeddings, and quarantined 89 malformed deliveries.
Replaying the feed preserved every company and embedding row and the top-five API response for
`SYN-001777`. The API returned HTTP 503 during a PostgreSQL outage and resumed serving recommendations
after recovery. The worker processed the retained delivery after PostgreSQL came back and resumed
after a RabbitMQ restart. A queued delivery and a quarantined message survived RabbitMQ container recreation,
and the restarted worker drained the source queue. See [DESIGN.md](DESIGN.md) for the design and trade-offs.

## Scaffold files

```
.
├── data/feed.jsonl        # the registry feed: 2,518 messages about 2,000 synthetic Malaysian companies, duplicates included
├── producer/publish.py    # replays the feed onto the queue
├── docker-compose.yml     # Postgres with pgvector, and RabbitMQ
└── Justfile               # local setup, worker, API, and shutdown commands
```

Add whatever else you need (`uv add` for libraries).

## How to submit

1. Click **Use this template** → **Create a new repository**, and make it **public**. Any name is fine.
2. Build your solution in your copy, with `DESIGN.md` at the top level.
3. When you're done, fill in the [submission form](https://docs.google.com/forms/d/e/1FAIpQLSdjCsXP5jFrKrIi4krLVS8Sq0rhM4mLlhycpsAo34sSj4h3FA/viewform) with a link to your repo and your résumé.

If the setup itself is broken (Docker, the services or the data won't start), open a
[Setup problem](https://github.com/OneCreditMY/mle-take-home/issues/new?template=setup-problem.yml) issue. Issues are public, so don't put your
solution or personal details in one.

## Time

Plan for **4 to 6 hours**, within 7 days of creating your copy, and stop at 6 hours. If you run out of time,
write down what you'd do next. A clear, honest "here's what's left" counts in your favour.

## What we look at

- Does it work, and does it keep working?
- How you tested it.
- How you reasoned about your choices and assumptions in `DESIGN.md`.
- How readable your code is.

We don't expect a perfect or complete solution.

## Using AI tools

Go ahead. Tell us in `DESIGN.md` which tools you used and for what. In the interview we'll ask you
to walk us through your code, so make sure you understand everything you submit.

## After you submit

- We review your work as soon as we can.
- If we move forward, the next interview is a walkthrough of your code with us.
- Either way, you'll get written feedback by email.
- Your repo stays yours. Keep it public as a portfolio piece if you like.

## About the data

Everything in this repo is synthetic. We generated it for this assessment.

- The companies, names, registration numbers, descriptions and events are made up. Any resemblance to a real company
  or person is a coincidence.
- No OneCredit customer data, credit data or production data is in this repo, and none was used to create it.
- Registration numbers use a made-up `SYN-` format, so they can't be mistaken for real Companies Commission of
  Malaysia (SSM) numbers.
- Your copy is public, so keep secrets, credentials and personal data out of it.
