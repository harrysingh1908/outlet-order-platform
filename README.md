# Outlet Order Platform — Production-Grade QSR Micro-Platform

> A micro-platform built to simulate the reliability, security, and deployment standards required for operating 2,600+ Quick Service Restaurant (QSR) outlets at enterprise scale (modelled after Americana DevOps/SRE standards).

---

## 🏗️ System Architecture

*(Architecture diagram will be expanded as each layer is built)*

```
+-----------------------------------------------------------------------+
|                              LAYER 1                                  |
|                                                                       |
|   [ Client / HTTP ]                                                   |
|           │                                                           |
|           ▼                                                           |
|    +--------------+      (Cache-Aside)     +---------------+         |
|    |  Order API   | ---------------------> |  Redis Cache  |         |
|    |   (FastAPI)  |                        +---------------+         |
|    +--------------+                                                   |
|       │        │                                                      |
|       │        └───────────────────────────+                          |
|       ▼ (Write Status: received)           ▼ (Produce Event)          |
|  +----------+                       +--------------+                  |
|  | MongoDB  |                       | Kafka Queue  |                  |
|  +----------+                       +--------------+                  |
|       ▲                                    │                          |
|       │ (Update: preparing -> ready)       ▼ (Consume Event)          |
|       +----------------------------- +------------------+             |
|                                      | Kitchen Worker   |             |
|                                      +------------------+             |
+-----------------------------------------------------------------------+
```

---

## 💡 Architectural Decisions ("Why X over Y")

| Component | Choice | Why Chosen | Alternative Considered & Trade-off |
|---|---|---|---|
| **API Framework** | FastAPI (Python) | High performance, async native, auto-generated OpenAPI docs. | Flask/Django: Flask requires third-party async; Django is too heavyweight for a lean microservice. |
| **Async Messaging** | Kafka (KRaft mode) | Decouples API from worker. High throughput queue absorbs lunch-rush order spikes. | Direct HTTP: Worker slowdowns or crashes would directly block the customer order response (`504 Gateway Timeout`). |
| **Primary Database** | MongoDB | Document format fits flexible restaurant order items and customizations without rigid schemas. | PostgreSQL: Relational joins are unnecessary for independent order documents. |
| **Caching Layer** | Redis | Ultra-low latency in-memory store for static, high-read endpoints (e.g. restaurant menu). | In-memory Python dict: Cannot be shared across multiple horizontal API pod replicas. |

---

## ⚠️ Known Limitations & Design Trade-offs

- **Dual-write non-atomicity (No Outbox Pattern)**: In `POST /orders`, writing to MongoDB and publishing to Kafka are two separate network operations. If Mongo succeeds but Kafka fails, the API marks the order as `failed` in Mongo and returns `503 Service Unavailable`. In a full enterprise system, the **Transactional Outbox Pattern** (using Debezium / CDC) would be used to guarantee atomic writes.

---

## 📊 Performance & Reliability Measurements

*(Numbers will be filled with real empirical measurements as tests are conducted)*

| Metric | Result | Target / Notes |
|---|---|---|
| Pipeline execution time (Push to GHCR) | *Pending Layer 2* | Shift-left security pipeline duration |
| p95 Latency under load | *Pending Layer 4* | Measured via k6 load test |
| HPA Scale-out time | *Pending Layer 3* | Measured during CPU spike |
| GitOps Rollback time | *Pending Layer 3* | Measured via `git revert` sync |
| Kafka Failure Recovery time | *Pending Layer 4* | Measured during chaos test |
| MongoDB Restore time | *Pending Layer 4* | Measured via `mongodump` & restore |
| Lynis Hardening Score | *Pending Layer 4* | Before vs After Ansible playbook execution |
| Test Environment Specs | *Pending Layer 4* | Local host specs |

---

## 🛠️ Quickstart

*(Commands will be added as layers complete)*

---

## 🗺️ Job Description Mapping

| Requirement | Project Implementation | Location in Repo |
|---|---|---|
| Containerization | Multi-stage Dockerfiles, non-root user, slim base images | `src/api/Dockerfile`, `src/worker/Dockerfile` |
| Queue / Messaging | Kafka single-node KRaft mode, idempotency, offset management | `src/api/`, `src/worker/` |
| NoSQL / In-Memory DB | MongoDB (orders & menu), Redis (menu cache TTL) | `src/` |
| CI/CD & Security | GitHub Actions, Hadolint, Gitleaks, Trivy CVE scanning | `.github/workflows/` |
| Kubernetes & GitOps | kind cluster, Helm charts, HPA, Probes, Argo CD | `deploy/` |
| SRE & Observability | Prometheus, Grafana 4 Golden Signals, SLO & Burn-rate alerts | `observability/` |
| Security & Hardening | Ansible playbook, SSH hardening, UFW, Lynis audit | `ansible/` |
