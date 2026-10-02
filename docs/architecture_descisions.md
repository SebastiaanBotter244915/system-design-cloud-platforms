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

### ADR-004: Compute Strategy

**Context.** Ingestion has to keep running when my laptop is closed. Today
that's two short hourly jobs for one corridor.

**Decision.** Run both images on one **EC2 t3.micro** (eu-west-1), started
hourly by cron with `docker run --rm`.

**Why a VM instead of a managed container service**
- The images already run with plain `docker run`, so there's no ECR, task
  definition or cluster to set up first.
- Two short jobs an hour don't need orchestration or autoscaling.
- The trade-off is that the OS, Docker and cron are mine to maintain, and
  if the VM dies both jobs stop.

**Cost:** about **$12.70/month** (t3.micro $8.32 + 8 GB gp3 $0.70 + public
IPv4 $3.65, on-demand, 730 h). The VM is idle about 58 minutes every hour.

**At 50 corridors every 5 minutes**
- Switch to **ECS Fargate tasks started by EventBridge Scheduler**: billed
  per run, no OS to patch, failed runs visible in CloudWatch.
- The NDW file covers the whole country, so one task downloads it and puts
  one message per corridor on **SQS** for a pool of parser workers.

**Operational concern I hadn't anticipated**
- The RDS security group only allowed my laptop's IP, so the VM timed out
  on port 5432 even though the same image worked locally. Fixed by allowing
  the VM's security group instead of an IP.

**Why the Redis queue (ADR-002) was dropped**
- Nothing consumes it yet, and on one VM it's just another container
  competing for 1 GB of RAM, with no durability.

### ADR-005: Compute & Deployment Strategy

**Context.** Day 5 adds a third image: the dashboard (`/site/{id}`,
`/health` and an HTML page). Unlike the two ingest jobs it's a
long-running web server, not a job that runs for a minute and exits.

**Decision.** Run the dashboard as a **third container on the same EC2
t3.micro**, started with `docker run -d --restart unless-stopped -p 8080:8000`.

**Why the same VM instead of a managed container service**
- It's one small FastAPI process serving one interchange to a handful of
  users. The VM is idle about 58 minutes every hour, so it has room.
- No extra cost: still about **$12.70/month**. A Fargate service plus a load
  balancer would add roughly $25–30/month for the same single container.
- The image runs exactly like the other two, so the deploy is the same
  `docker build` / `docker run` I already know.
- **What would change my mind:** the 99.5% SLO from ADR-003 actually being
  missed, needing HTTPS or more than one instance (then **ECS Fargate behind
  an ALB**, or App Runner), or the 1 GB of RAM running out once more
  corridors are added.

**How it runs long-term, and what happens on a reboot**
- `--restart unless-stopped` restarts the container if it crashes, and the
  Docker daemon is enabled at boot (`systemctl enable docker`), so after a
  VM reboot the dashboard comes back on its own. The cron jobs survive too,
  because the crontab is stored on disk.
- I chose the restart policy over a systemd unit: it's one flag instead of
  a unit file, and Docker already runs under systemd.
- The trade-off: nothing pulls a newer image. Deploying is still a manual
  `docker stop`, `docker rm` and `docker run` on the VM.

**What testing locally with Compose caught**
- **A port clash:** air-ingest's `/health` already uses host port 8000, so
  the dashboard is mapped to **8080**. That port also had to be opened in
  the VM's security group.
- **The model path:** `predict.py` loads `model/model.pkl` relative to
  itself, so the Dockerfile has to copy the pickle into `model/`, not next
  to the script.
- **What it didn't catch:** in Compose the ingest containers stay up, so
  `/health` can reach them by service name. On the VM they're `--rm` cron
  jobs, so most of the time the dashboard's `/health` reports them as
  unreachable.

**Operational concern: 1 GB of RAM**
- With three Python containers, the t3.micro ran out of memory and the
  kernel kept killing processes (confirmed in the instance's system log).
- First fix: cap the ingest containers (`--memory="150m"`) and stagger
  them in cron (:00 and :05). It didn't hold: the log still showed
  processes growing past 470 MB and being killed, so the cap wasn't
  actually enforced.
- Final fix: a **2 GB swap file**. A memory spike now means a slowdown
  instead of a kill, with `--restart unless-stopped` as a second line of
  defence. This is the first sign the VM is getting too small.

**Supersedes or extends ADR-004?**
- It **extends** ADR-004. Same VM, same region, same plain `docker run`, no
  orchestrator.
- What actually changed: the VM is no longer just a cron host. It now has
  an always-on process, an inbound port open to the internet, and less
  free memory. "If the VM dies, both jobs stop" now also means the
  dashboard goes down, which doesn't fit ADR-003's Warm Standby yet.

### ADR-006: ML Serving Architecture

**Context.** The dashboard needs a predicted NO₂ value and a
`no2_exceedance_risk` between 0 and 1 for each of the four NDW sites. The
training set (`training_data.csv`) has **6 hourly rows** so far.

**Decision.** A **LinearRegression** on `total_intensity_veh_per_hr` and
`hour_of_day`, trained offline (`predict.py`), saved as `model.pkl` and
**baked into the dashboard image**. Risk is a sigmoid on its prediction.

**Why a linear regression is enough model**
- 6 rows and 2 features. Anything more flexible (trees, boosting) would
  memorise the six points rather than learn from them.
- Plotting NO₂ against total intensity (`plots/`) doesn't show a linear
  relationship at all. With 6 points that's most likely too little data
  or plain noise, so it neither confirms nor rules out a linear model.
- **What would change that:** a few hundred hours (weeks of data) to add
  features like weekday and wind, which drive NO₂ more than traffic does.
  A non-linear model only makes sense with a few thousand hours (several
  months, covering seasons), and only if it beats the regression on CV.

**Evaluation: what the numbers do and don't tell me** (`evaluate.py`)
- Leave-one-out CV: **R² 0.42, MAE 6.6 µg/m³**, against R² −0.44,
  MAE 10.3 µg/m³ for always predicting the mean.
- **What they do tell me:** the model picks up *some* structure, since it
  beats a constant prediction.
- **What they don't:** with 6 points and 3 fitted parameters there are
  hardly any degrees of freedom left. The numbers say nothing about other
  seasons, weekdays or hours outside 12:00–20:00.
- The coefficients are unreliable. Intensity and hour are almost collinear
  (r = −0.97), so their effects can't be separated: hour's coefficient
  (−3.55) even has the opposite sign of its own correlation with NO₂
  (+0.77). I don't read them causally; more data across a wider range of
  hours and traffic levels should stabilise them.

**How risk is derived: threshold 40 µg/m³**
- 40 µg/m³ is the EU annual limit value (and the old WHO 2005 guideline).
  The WHO 2021 guideline is much stricter: 10 µg/m³ annual, 25 µg/m³ daily.
- These are **annual / daily averages**, and I'm applying them to **hourly**
  values. A single hour above 40 isn't a legal exceedance; the EU hourly
  limit is 200 µg/m³. So "risk" here means "this hour is high for this
  interchange", not "a limit is being broken".
- WHO's 10 or 25 would flag every hour I've measured (26–51 µg/m³), so it
  wouldn't tell hours apart. 40 splits my data (2 of 6 hours above).

**Risk formula:** sigmoid centred on 40 with steepness 0.2, applied to the
regression's prediction (`predict.py`).

**Stretch: LogisticRegression on a binary "> 40" label** (`compare_risk.py`)

| veh/hr | hour | actual NO₂ | exceeded | predicted NO₂ | sigmoid risk | logistic risk |
|-------:|-----:|-----------:|:--------:|--------------:|-------------:|--------------:|
| 2640 | 18 | 36.9 | no  | 35.2 | 0.28 | 0.16 |
| 1620 | 20 | 37.3 | no  | 44.0 | 0.69 | 0.54 |
| 4590 | 12 | 25.9 | no  | 26.1 | 0.06 | 0.02 |
| 2220 | 18 | 50.8 | yes | 41.8 | 0.59 | 0.40 |
| 1320 | 19 | 51.1 | yes | 52.2 | 0.92 | 0.85 |

- Both rank the hours the same way and both get the 20:00 row wrong (no
  exceedance, ~0.5–0.7 risk).
- The logistic model is more cautious, but it's trained on **2 positive
  examples**. Its probabilities aren't really calibrated, and with so few
  positives it can't learn much.
- **I trust the sigmoid-on-regression more for now.** The regression uses the
  actual NO₂ value of every row, not just above/below, so it gets more
  information from 6 rows. It's also one model to maintain, and the risk is
  easy to explain ("predicted 44, threshold 40"). The trade-off: steepness
  0.2 is a guess, not learned from data.
- Revisit once there are weeks of hourly data with enough exceedances to
  train and calibrate a classifier.

**Training-serving skew, and why the model is baked into the image**
- Skew means the model sees different inputs live than it saw in training.
  For example, `hour_of_day` in local time instead of UTC, or a reading that
  isn't rounded to the hour. The dashboard rounds the S3 timestamp in UTC
  the same way `build_training_data.py` does, to avoid this.
- **There is one real skew today:** the model was trained on the **total**
  of the four sites, but `/site/{id}` passes in **one** site's intensity.
  A single site's flow (300–2,000 veh/hr) is often below the lowest
  total the model has seen (1,320), so per-site predictions are
  extrapolations. Fix: pass the total, or retrain per site.
- Baking `model.pkl` into the image means the model, the feature code in
  `predict.py` and the pinned library versions ship and roll back together
  as one image tag. Retraining live on the VM could silently produce a
  different model than the one `evaluate.py` and `tests/test_model.py`
  checked, trained on whatever the database held at that moment.
- One remaining risk: the pickle was made with scikit-learn 1.9.0 locally,
  but the image pins 1.9.1. It should be trained inside the same image.

**Why one air-quality station (NL10240) is enough**
- All four NDW sites (`hrl`, `hrr`, `vwd`, `vwa`) are lanes and ramps of the
  **same A27 interchange**, a few hundred metres apart, and NL10240 sits next
  to it. The station measures the air those four sites create together, so
  it isn't standing in for somewhere it can't see.
- That's also why the model uses their **total** intensity: one NO₂ reading
  can't tell which ramp it came from.
- **What would force a second station:** a second interchange far enough
  away that its air isn't what NL10240 measures (a different road, or a
  different wind exposure). It would need its own station, training rows
  and model, otherwise its predictions are made from another place's air.

**What `/site/{id}` returns if `predict()` fails**
- The request still returns **200** with the real `no2_ug_m3` and
  `intensity_veh_per_hr`. Only `no2_ug_m3_predicted` and
  `no2_exceedance_risk` are `null`, which the page shows as "–".
- I chose this over a 500 because the measured values are the more
  important part, and they're fine: a broken model shouldn't take the
  real readings down with it, and it would count against the 99.5% SLO.
- The trade-off: a `null` is easy to miss. The exception is swallowed
  without a log line, so it should log a `predict_failed` error the same
  way ingestion logs `redis_publish_failed`.
