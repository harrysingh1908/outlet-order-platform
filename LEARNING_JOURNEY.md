# DevOps & SRE Master Learning Journey & Interview Guide

> **Project:** Outlet Order Platform (QSR Scale — Modeled for Americana 2,600+ Outlets)  
> **Purpose:** Concept-by-concept interview preparation paired with hands-on, production-grade implementation.

---

## 🎯 The 3-Step Pedagogical Methodology

For every single component, failure scenario, and architecture decision, we apply the **3-Step Ladder**:

1. **The Human Explanation (Intuition First):**
   - Zero buzzwords.
   - Real-world analogies (cashiers, ticket printers, whiteboard menus, elevator buttons).
   - Anchors the mental model before touching code or jargon.

2. **Why the DevOps Job Description (JD) Asks This:**
   - Maps directly to the target skills (Kubernetes, Kafka, DevSecOps, Observability, SRE).
   - Explains *why* interviewers test this specific edge-case or failure mode.

3. **The Plain-English Interview Answer:**
   - Conversational, authoritative, and confident.
   - Designed to be spoken aloud in 30–45 seconds during technical screening and architecture rounds.

---

## 🗺️ The Complete Concept Roadmap Across the 4 Layers

To complete the entire platform with full interview readiness, we are progressing through approximately **80 targeted concepts** across 4 layers (~20 concepts per layer):

| Layer | Focus Area | Concepts | Checkpoints |
|---|---|---|---|
| **Layer 1** | Application, Async Messaging (Kafka KRaft), DB & Cache (Mongo, Redis), Docker & Compose | Concepts 1–25 | Architecture, API, Worker, Dockerfiles, Compose, Live Chaos Edge-Cases, Pytest |
| **Layer 2** | DevSecOps CI/CD (GitHub Actions, Hadolint, Gitleaks, Trivy CVE scanning, GHCR, GitOps Tagging) | Concepts 26–40 | Pipeline stages, Shift-Left security, Vulnerability gating, Non-looping Git commits |
| **Layer 3** | Kubernetes (kind), Helm, HPA, Probes, NetworkPolicies, Argo CD GitOps & Rollbacks | Concepts 41–60 | Pod lifecycle, Resource limits, HPA math, Ingress, Calico/Cilium security, GitOps sync |
| **Layer 4** | SRE & Observability (Prometheus, Grafana 4 Golden Signals, SLOs, Burn-rate alerts, Chaos, Ansible) | Concepts 61–80 | Metrics scraping, Error budgets, Multi-window alerts, k6 load testing, Postmortem, Lynis hardening |

---

## 📚 Master Index of Concepts (So Far: Concepts 1–19)

### 🔹 Core Architecture & Microservices (Layer 1)
- **Concept 1: Startup & Shutdown Lifecycle (`lifespan`)**
  - *Why:* Prevents database connection leaks when containers restart under Kubernetes.
  - *Pitch:* FastAPI `lifespan` initializes pools on boot and drains them cleanly on shutdown.
- **Concept 2: Redis Menu Cache & DB Outage Graceful Degradation**
  - *Why:* Protects primary database from read exhaustion and preserves uptime during DB blips.
  - *Pitch:* Cache-Aside with 60s TTL delivers <2ms responses and lets `/menu` stay available even if Mongo is down.
- **Concept 3: The Kafka Failure in `POST /orders` (Broken Printer Trap)**
  - *Why:* Prevents phantom orders and silent data loss in distributed systems.
  - *Pitch:* If Kafka publish fails, we update Mongo to `failed` and return HTTP 503 rather than falsely returning 202.
- **Concept 4: Worker Idempotency & Manual Offset Commits**
  - *Why:* Kafka guarantees at-least-once delivery; duplicate messages will occur.
  - *Pitch:* Worker checks DB status (`ready`) before processing to skip duplicates, and commits offsets only *after* DB write succeeds.

### 🔹 Messaging & Distributed Scaling
- **Concept 5: Kafka Consumer Groups**
  - *Why:* Enables horizontal worker autoscaling without duplicate task execution.
  - *Pitch:* Adding worker pods shares partitions automatically within the `kitchen-workers` group.
- **Concept 6: Startup Retries & CrashLoopBackOff**
  - *Why:* Services boot asynchronously; prevents pods from crashing before dependencies are ready.
  - *Pitch:* Exponential backoff loops allow workers to wait cleanly for Kafka and MongoDB without failing pod health.
- **Concept 7: Realistic State Machine (`received` ➔ `preparing` ➔ `ready`)**
  - *Why:* Simulates asynchronous event-driven workflow visible through real-time API status polling.
- **Concept 11: Async/Await & Event Loop Concurrency**
  - *Why:* Allows a single process to handle thousands of concurrent I/O operations without thread starvation.
  - *Pitch:* Non-blocking I/O frees the event loop while waiting on Kafka and MongoDB network packets.
- **Concept 12: `SIGTERM` vs `SIGKILL` (Graceful Pod Termination)**
  - *Why:* Kubernetes gives 30s grace period; worker must finish in-flight orders and flush offsets before dying.
- **Concept 13: Consumer Lag (The #1 SRE Kafka Metric)**
  - *Why:* The gap between latest offset and committed offset reveals worker bottlenecks before outages happen.

### 🔹 Containerization & Orchestration (Docker & Compose)
- **Concept 8: Multi-Stage Docker Builds**
  - *Why:* Keeps images slim (~120MB) and strips build tools/compilers to shrink the CVE attack surface.
- **Concept 9: Non-Root Container Execution (`USER appuser`)**
  - *Why:* Security standard (passes Hadolint rule DL3002) preventing container breakout privileges.
- **Concept 10: Docker Compose Health Checks (`depends_on: condition: service_healthy`)**
  - *Why:* Guarantees upstream dependencies (Kafka/Mongo/Redis) are fully operational before dependent apps boot.
- **Concept 14: Docker Internal DNS & Bridge Networking**
  - *Why:* Containers resolve peer services by name (`kafka:9092`) via embedded DNS, not `localhost`.
- **Concept 15: Twelve-Factor Config (Environment Variable Ingestion)**
  - *Why:* Separates code from environment configs so the identical image runs across Dev, Stage, and Prod.
- **Concept 16: Docker Layer Caching Strategy**
  - *Why:* Copying `requirements.txt` before source code avoids reinstalling packages on every small code change.
- **Concept 17: Container Ephemerality & Named Volumes**
  - *Why:* Container filesystems are destroyed on stop; named volumes persist database state across restarts.
- **Concept 18: `EXPOSE` vs `ports:` Publishing**
  - *Why:* `EXPOSE` is purely documentation; `ports:` actively binds container ports to the host interface.
- **Concept 19: Health Check Parameters (`start_period`, `interval`, `retries`)**
  - *Why:* Gives heavy services like Kafka bootstrap grace time before applying failure thresholds.
- **Concept 20: Docker Architecture (CLI vs Daemon & Sockets)**
  - *Why:* Docker CLI is a REST client that communicates with the background daemon (`dockerd`) over Unix sockets/named pipes.
- **Concept 21: Unit Tests vs Integration Tests (Shift-Left Testing)**
  - *Why:* Unit tests run in under 1 second without external databases, failing fast in CI before expensive Docker image builds.
- **Concept 22: Mocking External Dependencies (`unittest.mock`)**
  - *Why:* Isolates unit tests from live databases and queues so tests run deterministically anywhere.
