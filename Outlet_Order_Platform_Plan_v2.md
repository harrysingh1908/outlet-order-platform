# Outlet Order Platform — Build Plan v2

**Goal:** One portfolio project that covers the core of the Americana DevOps JD (Kubernetes, CI/CD, GitOps, monitoring, SRE basics, Kafka, MongoDB, Linux hardening), built in 4 layers.

**The story for the interview:** "Americana runs 2,600+ restaurant outlets. I built a small version of the platform those outlets would run on, using the same reliability, security and deployment practices your JD asks for."

**Golden rule:** write in the README as you build. The README gets you the interview. The code gets you through it.

---

## RULES FOR THE AI HELPING WITH THIS PROJECT (read first)

If you are an AI assistant helping the student build this, follow these rules:

1. **Follow this plan. Do not add tools, layers or "improvements" that are not written here.** If you think something should change, say so and ask first.
2. **Out of scope (do not suggest or add):** Terraform, any Azure/AKS/cloud deployment, Jenkins, Packer, ARM, New Relic, Aerospike, service mesh, Vault, Kubernetes operators, multi-cluster, Redis clusters, extra microservices.
3. **Keep it simple.** Explain every step in plain words. If a term is new, define it in one line first. The student must be able to explain every line in an interview.
4. **Work one layer at a time, in order.** Do not start the next layer until the "Definition of done" boxes of the current one are checked.
5. **Never invent numbers.** Results (times, latency, scores) must come from the student's real runs. If a number does not exist yet, leave it blank.
6. **Do not tick a skill as done until it is built and tested.** Honest small claims beat big claims that fall apart in an interview.
7. **When something breaks, explain why it broke** before giving the fix. The student needs the lesson, not just the fix.
8. **Keep code small and readable.** No clever tricks. The student must be able to read and defend all of it.

---

## Quick glossary (plain words)

- **Container / Docker image:** your app plus everything it needs, packed so it runs the same everywhere.
- **Kubernetes (K8s):** a system that runs containers, restarts them when they die, and adds more when traffic grows.
- **kind:** runs a small real Kubernetes cluster inside Docker on your laptop. Free.
- **Helm:** templates for Kubernetes files, so one chart can be reused with different settings.
- **Pod:** the smallest thing Kubernetes runs (usually one container).
- **CI/CD:** CI = automatically test and build on every push. CD = automatically deploy.
- **GitOps (Argo CD):** Git is the single source of truth. Argo CD watches Git and makes the cluster match it.
- **Kafka:** a queue. The API drops orders in; the worker picks them out at its own speed.
- **SLI / SLO:** SLI = a measurement (e.g. % of requests that succeed). SLO = the target for it (e.g. 99.5%).
- **Error budget:** how much failure the SLO allows. 99.5% target means 0.5% of requests are allowed to fail.
- **Burn rate:** how fast you are using up the error budget. 1x = you'll use exactly all of it by the end of the period. 14.4x = very fast, wake someone up.
- **Chaos testing:** break things on purpose to see what happens.
- **Idempotent:** doing the same action twice has the same result as doing it once.

---

## Layer 1 — The Application

### What you're building
A food-ordering system with 4 small pieces:

1. **Order API** (Python + FastAPI), 3 endpoints only:
   - `POST /orders` — place an order
   - `GET /orders/{id}` — check order status
   - `GET /menu` — view the menu
2. **Kafka** — one topic called `orders`. The API sends each new order into it.
3. **Kitchen worker** (small Python script) — picks orders from Kafka and updates the status: `received` → `preparing` → `ready`.
4. **MongoDB** stores orders and the menu. **Redis** caches the menu.

### Exact behaviour (follow this so the app doesn't break)
- **`POST /orders`:**
  1. Save the order in MongoDB with status `received`.
  2. Send it to Kafka.
  3. Return `202 Accepted` with the order id.
  4. If sending to Kafka fails: mark the order `failed` and return `503` with a clear message. Never return success for an order that wasn't queued.
- **`GET /orders/{id}`:** reads from MongoDB. Because step 1 above saves the order first, this never says "not found" right after placing an order.
- **`GET /menu`:** check Redis first. If missing, read from MongoDB, then store in Redis with an expiry time (TTL, e.g. 60 seconds). Seed the menu into MongoDB at startup. If both Redis and MongoDB are down, return `503`.
- **Worker:**
  - Before updating an order, check its current status. If it was already processed, skip it. This makes it **idempotent** (Kafka can deliver the same message twice; this is called "at-least-once delivery").
  - Save the Kafka offset (its "I've read up to here" marker) only **after** the MongoDB write succeeds.
  - If Kafka or MongoDB isn't ready when it starts, retry instead of crashing.

### Known limitation (write it in the README, it shows maturity)
Saving to MongoDB and sending to Kafka are two separate steps; one can succeed while the other fails. The real-world fix is called the "outbox pattern". You are not building it. You handle the failure case (mark `failed`, return `503`) and you know the better solution exists.

### Why it's built this way (learn this — interview gold)
- **Why Kafka between API and worker?** A lunch-rush spike doesn't overwhelm the kitchen. The queue absorbs the burst and the worker processes at its own pace. This is "decoupling".
- **Why MongoDB?** Orders are documents with variable fields (items, customizations), which fits a document database.
- **Why Redis?** The menu rarely changes but is read constantly: a perfect cache candidate. It covers "in-memory databases" from the JD.
- **Why FastAPI?** You know Python; it gives auto-generated API docs and good speed.

### Kafka scope (don't over-build)
One single-node Kafka (KRaft mode, so no separate Zookeeper), one topic, one producer (API), one consumer group (worker). But **learn these 4 cold:** topic, partition, consumer group, offset. Interviewers probe these to check if you really used Kafka.

### Docker
- One `Dockerfile` for the API, one for the worker.
- **Multi-stage build** (build in stage 1, copy only what's needed into stage 2, so the image is small).
- **Slim base image** (e.g. `python:3.x-slim`) and **run as a non-root user**. Security interviewers always ask about this.
- `docker-compose.yml` that starts everything (API, worker, Kafka, MongoDB, Redis) with one command. Use health checks so the API/worker wait for Kafka and MongoDB.

### Definition of done
- [ ] `docker compose up` starts the whole system
- [ ] Place an order, then watch its status change as the worker processes it
- [ ] `GET /orders/{id}` works immediately after `POST /orders`
- [ ] Stop MongoDB after the menu was cached: `/menu` still works until the cache expires
- [ ] Stop Kafka: `POST /orders` returns a clear `503`, nothing is lost silently
- [ ] Send the same Kafka message twice: the order is not processed twice
- [ ] Basic pytest unit tests for the API
- [ ] README: architecture diagram (draw.io or Excalidraw) + "why I chose each component" + the known limitation above

### Skills this layer honestly proves
Python | Docker | Kafka (basic) | MongoDB | Redis (in-memory DB) | Basic distributed-system thinking

---

## Layer 2 — The CI/CD Pipeline (GitHub Actions)

### What you're building
One GitHub Actions workflow that runs on every push and pull request. In order:

1. **Hadolint** — checks your Dockerfiles for bad practices.
2. **Test** — runs pytest. Fails → pipeline stops.
3. **Gitleaks** — scans for accidentally committed secrets (passwords, API keys). Fails → stops.
4. **Build** — builds the Docker images.
5. **Trivy** — scans the images for known vulnerabilities (CVEs). **Fails the pipeline on CRITICAL.** Configure it to **ignore issues that have no fix yet**, otherwise some base images block you forever and you can't do anything about it.
6. **Push** — pushes passing images to GitHub Container Registry (GHCR), tagged with the **commit ID** (not `latest`).
7. **Update the deploy file** — edits the image tag inside the Helm values file in the repo and commits it. This is how Argo CD (Layer 3) learns there is a new version. **Without this step nothing ever gets deployed.**
   - Make sure this commit does **not** trigger the pipeline again (use `[skip ci]` in the message or `paths-ignore`), otherwise it loops forever.

### Why this order matters (interview answer)
Security scans run **before** anything is published ("shift left"). A vulnerable image never reaches the registry. This is the DevSecOps mindset, and it connects to your Security+ and SOC background.

### Extra (cheap)
Cache Docker layers so repeat builds are fast.

### Measure
Time the full pipeline from push to a secured image in the registry. Write the number down for the results table.

### Definition of done
- [ ] Push bad code → pipeline fails at tests
- [ ] Commit a fake API key → pipeline fails at Gitleaks. **Do this on a throwaway branch with an obviously fake key, and never merge it.**
- [ ] Add a vulnerable dependency → pipeline fails at Trivy
- [ ] Push good code → image appears in GHCR with the commit tag
- [ ] After a good push, the Helm values file in Git gets the new tag automatically, with no infinite loop
- [ ] README: pipeline diagram + screenshots of the 3 failures

### Skills this layer honestly proves
GitHub Actions / CI-CD | DevSecOps | Container security | Automation

---

## Layer 3 — Kubernetes + GitOps

### Step 1 — Local cluster (do these two things on day one)
- Install `kind`. Create the cluster with a **config file that maps ports 80/443** so your browser can reach the Ingress. Doing this later means deleting and recreating the cluster.
- Install **metrics-server**. The autoscaler (HPA) needs it to read CPU usage, and kind doesn't include it. On kind it also needs the `--kubelet-insecure-tls` flag. Without it, the HPA shows "unknown" and never scales.

### Step 2 — Deploy the app
- Write your own **Helm charts** for the API and the worker.
- **Kafka, MongoDB, Redis:** keep them simple. Single-node Kafka, and small plain manifests (or very simple charts) for MongoDB and Redis. Before using a popular public chart (e.g. Bitnami), check that it still works; as far as I know, Bitnami restricted its free images in 2025 and many tutorials broke.
- **Kafka and MongoDB need a persistent volume (PVC).** Without it, killing the pod deletes the data and your chaos tests mean nothing.

Your charts must include:
- **Probes** (liveness + readiness). Liveness = "is it alive, restart if not". Readiness = "is it ready for traffic, don't send requests yet if not". Classic interview question.
- **Resource requests & limits** — how much CPU/memory a pod is guaranteed vs capped at. The HPA needs requests to work.
- **HPA** — automatically adds API pods when CPU goes up. Set the CPU target low enough (e.g. 50%) that your load test can trigger it.
- **Secrets** — database credentials as Kubernetes Secrets, never hardcoded. **Create them by hand once** with `kubectl create secret ...` and keep them out of Git. Argo CD deploys whatever is in Git, and secrets must never be committed (your own Gitleaks would catch it). Write one README line: "Secrets are created manually here; in production I'd use Sealed Secrets or a vault."
- **Ingress** — routes outside traffic into the cluster.
- **NetworkPolicy** — the correct rules are:
  - API → can talk to Kafka (send), MongoDB (read/write), Redis
  - Worker → can talk to Kafka (receive), MongoDB (write)
  - Nothing from outside reaches the worker
  - Everything else is denied
  - **Test it, don't assume it:** kind's default network plugin may not enforce NetworkPolicy depending on the version. Try to make a blocked connection; if it still works, install Calico or Cilium and test again.

### Step 3 — Argo CD (GitOps)
Install Argo CD and point it at your Git repo. From now on you don't deploy by hand. The pipeline updates the image tag in Git (Layer 2, step 7), and Argo CD syncs the cluster to match.

**The demo moment:** push a deliberately broken release (bad image tag or broken config). Watch it fail. Roll back with a single `git revert`. **Time the rollback.**

### Why each piece (interview answers)
- **Helm:** one chart, different settings per environment.
- **Probes:** without readiness probes, traffic hits pods that aren't ready, giving errors during deploys.
- **Requests/limits:** without them, one hungry pod starves the node, and the HPA has nothing to measure against.
- **GitOps vs plain CI/CD:** plain CI/CD *pushes* changes into the cluster (needs cluster credentials in CI). GitOps *pulls* from inside the cluster: it is auditable, and it undoes manual tampering. Be ready to explain this.

### Definition of done
- [ ] One command (`make deploy`) brings the whole app up on kind, reachable through Ingress
- [ ] Kill an API pod manually → Kubernetes restarts it
- [ ] Load burst → HPA adds pods, then scales back down
- [ ] NetworkPolicy test: a blocked connection is really blocked
- [ ] Change a value in Git → Argo CD syncs it automatically
- [ ] Good push → pipeline updates the tag → Argo CD deploys it, end to end with no manual step
- [ ] Broken-release rollback recorded (screen recording), rollback time written down
- [ ] README: Kubernetes architecture diagram + GitOps flow diagram

### Skills this layer honestly proves
Kubernetes | Helm | Argo CD / GitOps | Basic high availability | Network security

---

## Layer 4 — Reliability, Observability & Operations (SRE layer)

This is the differentiator. Layers 1–3 make you a DevOps candidate. Layer 4 makes you an SRE candidate, which is what the JD's "Your Impact" section describes.

### Part A — Observability
1. Install **kube-prometheus-stack** (Helm chart: Prometheus + Grafana + Alertmanager in one). It uses a fair amount of RAM on a laptop, so close other heavy apps.
2. Add metrics to the API (the `prometheus-fastapi-instrumentator` package: a few lines).
3. Create a **ServiceMonitor** so Prometheus actually scrapes the API. Forgetting this is the most common reason "my metrics are empty".
4. Build **one good Grafana dashboard** with the Four Golden Signals:
   - **Latency** (track p95)
   - **Traffic** (requests per second)
   - **Errors** (error rate %)
   - **Saturation** (CPU/memory vs limits)

### Part B — SLOs and error budgets (the JD names these)
1. Define **one SLO**: *"99.5% of order requests succeed over a rolling 30 days."* Count 5xx responses as failures. Don't count 4xx (those are the caller's mistake).
2. Write the **error budget math** in the README. Label it as an example: "if there were 1M requests, 0.5% = 5,000 allowed failures". The budget is *permission to fail*, not just a target.
3. Set up **burn-rate alerts** that check two windows together (a long one and a short one), so the alert stops quickly once the problem is fixed:
   - **Fast burn (page someone):** 14.4x burn over 1 hour, confirmed by the last 5 minutes
   - **Slow burn (open a ticket):** 1x burn over 3 days, confirmed by the last 6 hours
4. **You can't wait 3 days to prove an alert works.** For the demo, temporarily shrink the windows (e.g. 5 and 30 minutes), cause errors on purpose, show the alert firing, then put the real values back. Say this clearly in the README.
5. Write a **short error budget policy**: "When the budget is used up, feature releases freeze and only reliability work ships until it recovers."

### Part C — Load + chaos testing (this generates your resume numbers)
**Note:** k6 and the cluster run on the same laptop and compete for CPU. Write your laptop specs next to the results and say "measured on a local laptop".

1. **Load test with k6:** ramp traffic up slowly. Record p95 latency, watch the HPA scale out, and record how long scale-out took.
2. **Three chaos experiments**, one at a time. Write down what actually happens:
   - **Kill API pods during load:** does traffic survive? How long to recover?
   - **Kill the Kafka pod:** the *expected correct* result is that the API returns clear `503` errors (it can't queue anything), no already-accepted order is lost, and after Kafka returns the system recovers. Check this.
   - **Kill MongoDB:** does the API fail gracefully (clear errors) or crash? Does it recover on its own?
3. **Fix one weakness you found** (you will find at least one: for example missing retry logic, or a pod that hangs instead of failing). The fix and the story are worth more than the test itself.
4. **Small backup/restore test (about 30 minutes):** `mongodump`, wipe the database, restore, and time it. This is your honest recovery-time number.

### Part D — Postmortem + runbooks
- **One blameless postmortem** from your most interesting chaos test: what happened, timeline, root cause, what you changed, how you'll prevent it. ("Blameless" = focus on the system, not on who made a mistake.)
- **Two runbooks** (step-by-step guides for whoever is on call): "Orders stuck in Kafka queue" and "API error budget burning fast". Format: symptoms → commands to diagnose → fix steps.

### Part E — Ansible hardening (Linux hardening + automation)
- Make a real Linux VM with **Multipass or VirtualBox**. Containers don't work for this, and don't run it on your own laptop OS.
- One Ansible playbook that:
  - locks down SSH (no root login, key-only login). **Keep a second SSH session open while testing, so you can't lock yourself out.**
  - sets firewall rules (ufw)
  - installs fail2ban and auditd
  - installs node_exporter (a small program that exposes the VM's CPU/memory/disk numbers)
- Run **Lynis** (a security audit tool) **before and after** the playbook. The score improvement is a clean, honest number.
- Showing node_exporter output with `curl` on port 9100 is enough. Connecting it to Grafana is optional.

### Definition of done
- [ ] Grafana dashboard screenshot in README with all four golden signals
- [ ] SLO + error budget math in README
- [ ] Burn-rate alert fires when you deliberately cause errors (proof/screenshot), with the demo-window note
- [ ] Results table filled with real numbers
- [ ] Three chaos experiments written up, one weakness fixed
- [ ] Backup/restore time recorded
- [ ] One postmortem + two runbooks committed
- [ ] Lynis before/after scores recorded

### Skills this layer honestly proves
Prometheus/Grafana | SLO/SLI/error budgets | Alerting strategy | Chaos testing | Incident write-ups & runbooks | Ansible | Linux hardening | Basic recovery testing

---

## Build order and what to cut if time runs short

**Build order:** Layer 1 → Layer 2 → Layer 3 → Layer 4 (A, B, C, D, then E).

**If time runs short, cut in this order:**
1. Part E (Ansible)
2. The backup/restore test
3. The third chaos experiment (MongoDB)

**Never cut:** Layers 1, 2 and 3, or the SLO/burn-rate part of Layer 4.

**Deadlines (fill in once the drive date is known):**
- Layer 1 done by: ____
- Layer 2 done by: ____
- Layer 3 done by: ____
- Layer 4 done by: ____
- README polished + demo video by: ____
- Theory revision (last days before the drive): ____

---

## Honest gaps (what this project does NOT cover)
Be upfront about these in the resume and interview. Don't claim them.

- **Azure:** only AZ-900 (fundamentals). Say: "I know the AWS side well and I can map the services to Azure equivalents."
- **Terraform:** not used in this project. Your AWS SIEM lab shows it separately.
- **Jenkins, Packer, ARM templates, New Relic, Aerospike:** know what each one does and where it fits. "Haven't used it, here's how I'd learn it."
- **Networking (DNS, NAT, routing, subnetting):** not part of the project. Study it separately, since the JD lists it.
- **Large-scale e-commerce experience:** nobody expects this from a fresher.

---

## The README (assemble as you build, polish last)

1. **One-paragraph pitch:** "a small version of a QSR order platform, built to production-style practices"
2. **Architecture diagram**
3. **"Why I chose X over Y"** section (this is what recruiters actually read)
4. **SLO + error budget math**
5. **Results table.** Fill ONLY with numbers you measured:

| Metric | Result |
|---|---|
| Pipeline time (push → secured image) | |
| p95 latency under load | |
| HPA scale-out time | |
| Rollback time (GitOps) | |
| Recovery time per chaos test | |
| MongoDB restore time | |
| Lynis hardening score (before → after) | |
| Laptop specs used for testing | |

6. **"Maps to the job" table:** JD requirement → where it is shown in this repo, plus a "not covered" column (see Honest gaps)
7. **Quickstart:** one command (Makefile) to run everything
8. **2-minute demo video** link

## Repo hygiene
- Clear, small commits with real messages (reviewers scroll the history)
- A **Makefile** with `make up`, `make test`, `make deploy`, `make load-test`
- No secrets anywhere (your own Gitleaks will catch you)

## Resume bullets (write only after the numbers exist)
- "Built a Kafka-based order platform on Kubernetes with GitOps (Argo CD); rolled back a bad release in under X min"
- "Built a DevSecOps pipeline (GitHub Actions, Gitleaks, Trivy) that blocks critical CVEs before publish, end to end in X min"
- "Defined an SLO with multi-window burn-rate alerts; tested 3 chaos scenarios, recovering in under X s"
- "Hardened a Linux host with Ansible, raising the Lynis score from X to Y"

Resume skills line (update only after each layer is really done): Docker, Kubernetes, Helm, GitHub Actions, Argo CD, Prometheus, Grafana, Ansible, Kafka (basic), MongoDB, Redis, Linux, Python.
