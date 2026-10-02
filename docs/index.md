<div align="center">

# 🌬️ AirBreda

**Does A27 congestion near Breda drive NO₂ exceedances?**

*A cloud system design project on real Dutch open data, from ingestion to prediction.*

<a href="https://github.com/SebastiaanBotter244915/system-design-cloud-platforms"><img src="https://img.shields.io/badge/GitHub-Repository-1e5aa8?style=for-the-badge&logo=github&logoColor=white" alt="GitHub repository"></a>
<a href="architecture_descisions.md"><img src="https://img.shields.io/badge/Architecture-Decisions-2b7bd6?style=for-the-badge&logo=readthedocs&logoColor=white" alt="Architecture Decisions"></a>
<a href="architecture_design.md"><img src="https://img.shields.io/badge/Architecture-Design-4a9ae8?style=for-the-badge&logo=diagramsdotnet&logoColor=white" alt="Architecture Design"></a>

![AWS](https://img.shields.io/badge/AWS-EC2_·_RDS_·_S3-1e5aa8?style=flat-square&logo=amazonwebservices&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-3_containers-2b7bd6?style=flat-square&logo=docker&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.11-4a9ae8?style=flat-square&logo=python&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-RDS-1e5aa8?style=flat-square&logo=postgresql&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-dashboard-2b7bd6?style=flat-square&logo=fastapi&logoColor=white)
![scikit-learn](https://img.shields.io/badge/scikit--learn-LinearRegression-4a9ae8?style=flat-square&logo=scikitlearn&logoColor=white)

</div>

---

## 📌 About the project

AirBreda combines two Dutch open-data sources: hourly **NO₂** from RIVM's
**Luchtmeetnet** (station NL10240) and per-minute **traffic** from **NDW**
(four sites on the A27 interchange near Breda). Two ingestion containers
store the readings in **RDS PostgreSQL** and **S3** every hour. A third
container serves a dashboard with the measured NO₂, traffic per site and a
predicted **exceedance risk**.

Everything runs on **one EC2 VM** in AWS, and the same images run locally
with `docker compose up`.

> 💡 The two documents below are the core of this project. The **decisions**
> explain *why* the system looks the way it does, the **design** shows *what*
> it looks like at every checkpoint.

---

## 📚 Documentation

| | Document | What you'll find |
|:-:|---|---|
| 🧭 | [**Architecture Decisions**](architecture_descisions.md) | Six ADRs: context, decision, alternatives and trade-offs |
| 🗺️ | [**Architecture Design**](architecture_design.md) | Diagrams of the system at each checkpoint, from first plan to full system |

### 🧭 Architecture Decisions (ADRs)

| ADR | Topic | Decision in one line |
|:---:|---|---|
| [001](architecture_descisions.md#adr-001-initial-data-storage-strategy) | Data storage | Raw files in **S3**, parsed readings in **PostgreSQL**, idempotent upserts |
| [002](architecture_descisions.md#adr-002-messaging-architecture) | Messaging | **Redis** queue to decouple ingestion from consumers |
| [003](architecture_descisions.md#adr-003-resilience-strategy) | Resilience | **99.5%** SLO with a **Warm Standby** DR tier |
| [004](architecture_descisions.md#adr-004-compute-strategy) | Compute | One **EC2 t3.micro**, ingestion started hourly by cron |
| [005](architecture_descisions.md#adr-005-compute--deployment-strategy) | Deployment | Dashboard as a **third container** on the same VM |
| [006](architecture_descisions.md#adr-006-ml-serving-architecture) | ML serving | **Linear regression** baked into the image, sigmoid for risk |

### 🗺️ Architecture Design (checkpoints)

| Checkpoint | State of the system |
|:---:|---|
| [**1**](architecture_design.md#1-architecture-diagram-checkpoint-1) | First plan: ingestion with a raw S3 zone |
| [**2**](architecture_design.md#2-architecture-diagram-checkpoint-2--patterns--resilience) | Local Docker Compose stack: queue, logging, data-quality checks |
| [**3**](architecture_design.md#3-architecture-diagram-checkpoint-3--datalab-state) | Deployed to AWS: EC2 + RDS + S3, IAM instance role |
| [**4**](architecture_design.md#4-architecture-diagram-checkpoint-4--full-airbreda-architecture) | ⭐ **Full system:** dashboard, ML model, least-privilege IAM |

---

## ⚙️ System at a glance

```
 Luchtmeetnet ──▶ airbreda-air ─────┐
                                    ├──▶ RDS PostgreSQL ──┐
 NDW ───────────▶ airbreda-traffic ─┤                     ├──▶ airbreda-dashboard ──▶ 🌐 :8080
                                    └──▶ S3 bucket ───────┘      (model.pkl inside)
```

<sub>All three containers run on one EC2 t3.micro (eu-west-1). Full diagram in
[Checkpoint 4](architecture_design.md#4-architecture-diagram-checkpoint-4--full-airbreda-architecture).</sub>
