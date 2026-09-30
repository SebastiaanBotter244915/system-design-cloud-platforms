# AirBreda — Architecture Descision Document
### ADR-001: Initial Data Storage Strategy

**Context.** Both sources produce small, structured time series: hourly NO2
from Luchtmeetnet and per-minute traffic from NDW. The main query joins the
two on time.

**Decision.** Keep raw payloads in **Amazon S3** and parsed readings in
**Amazon RDS for PostgreSQL**.

**Why relational (PostgreSQL) instead of NoSQL**
- Fixed, known schema (station, component, timestamp, value), so a flexible schema adds nothing.
- The workload is SQL-shaped: join air and traffic on time and aggregate
  1-minute traffic into hours.
- At thousands of rows per day, NoSQL's horizontal scaling isn't needed.

**Why raw files are stored in object storage**
- **Replay:** parsing logic will change, and NDW only serves current data, so
  S3 is the only way to rebuild the database after a fix.
- **Cost:** large, write-once blobs are cheap in S3 and would bloat PostgreSQL.

**How duplicate readings are handled**
- Delivery is **at-least-once**. Luchtmeetnet returns the same latest hour
  on every call, and runs can be retried.
- Writes upsert on `UNIQUE (station_id, component, timestamp_measured)` with
  `INSERT ... ON CONFLICT DO UPDATE`, so a duplicate leaves the value
  unchanged. Exactly-once delivery was rejected as over-engineering: the
  idempotent write makes duplicates harmless.

**Alternative considered and rejected:** 
- Storing raw payloads in PostgreSQL (`JSONB`/`bytea`). It would be simpler, but NDW files are large and would bloat
  storage and backups.

**Reflection (What architectural decision from today would you be least confident
defending in a design review?)**
- Storing raw payloads in S3 for replay. It's the main argument for the
  bucket, but what's actually built stores parsed traffic CSVs and nothing
  for air. If the parser has a bug, the CSVs carry the same bug, so the
  database can't really be rebuilt from S3 yet. Until the raw `.xml.gz` and
  JSON are archived as received, that promise isn't kept.

### ADR-002: Messaging Architecture

**Context.** Both ingestion services produce readings that more than one
consumer may need: the database today, and later an ML model, an anomaly
detector or a dashboard.

**Decision.** Each service publishes every reading to a shared **Redis**
list (`readings`), alongside its direct database write.

**Why a queue instead of only direct database writes**
- It decouples the ingestion scripts from whoever consumes the data. A new
  consumer just reads the queue, so the ingestion code doesn't need to know
  about it or be changed to add it.

**Why Redis (over SQS or Service Bus)**
- It runs as a single container next to the existing services with zero
  cloud setup, which is ideal for learning the queue pattern itself.
- If the project grew, I'd evaluate SQS or Service Bus: a managed broker
  adds durability and monitoring that Redis doesn't give you out of the box.

**What happens if the broker goes down**
- The database write still succeeds, because the DB and Redis writes are
  separate try/except blocks. No data is lost at the source of truth.
- Messages meant for Redis during the outage are permanently dropped: there
  is no retry or replay once it recovers.
- Structured logging (`redis_publish_failed`, ERROR) at least makes the
  failure visible rather than silent.

**Bad data: flag-and-keep (Luchtmeetnet) vs drop (NDW)**
- A NO2 null is a real timestamp with missing data, so flagging it
  (`is_flagged = TRUE`) and keeping it avoids gaps in the series.
- NDW's -1 is a sentinel that would corrupt averages, so that row is dropped.
- Starting over, I'd consider flag-and-keep for NDW too, since dropping
  still leaves the same gap problem.

### ADR-003: Resilience Strategy

**Context.** AirBreda will serve readings per site through a `/site/{id}`
endpoint (not built yet). The data is hourly, so short outages cost little,
but the dashboard should be reliably available.

**Decision.** An SLO of **99.5%** for `/site/{id}`, backed by a
**Warm Standby** DR tier.

**SLO: 99.5%**
- That allows about 3.6 hours of downtime per month. Readings only change
  hourly, so the stricter 99.9% (about 43 minutes a month) would cost more
  than it's worth.

**DR tier: Warm Standby**
- An air-quality dashboard does not need Active-Active. A Warm Standby with
  a **15-minute RTO** and **1-hour RPO** is sufficient and cost-justifiable.
- A 1-hour RPO matches the hourly polling: at worst, one poll is lost.

**Cost versus the next tier up (Active-Active)**
- Warm Standby only pays for a scaled-down duplicate, scaled up on failover.
- Active-Active runs full capacity in two regions permanently, so it costs
  roughly double.