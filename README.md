# URL Shortener

A Python URL shortener with cached redirects, durable click analytics, operational metrics, and a separately usable, governed SDLC workflow engine.

## Architecture and behavior

- **API:** FastAPI, async PostgreSQL access, and Redis read-through caching. Generated codes use a cryptographically secure random alphabet. Custom codes accept letters, digits, `_`, and `-`.
- **Redirect and analytics durability:** The API returns a redirect only after Redpanda acknowledges the click event (`acks=all`, producer idempotence enabled). If the broker cannot acknowledge it within the configured bound, the request gets `503` rather than silently losing analytics. This is an intentional consistency-over-availability choice.
- **Analytics persistence:** A Kafka consumer group writes event IDs to PostgreSQL using a primary key and `ON CONFLICT DO NOTHING`; it commits offsets only after the database write. Replayed events therefore do not inflate click counts. Invalid events go to `redirect-events.dlq`. Referer headers are deliberately not collected.
- **Scaling:** Nginx resolves multiple API container addresses; the topic is provisioned with 12 partitions so worker replicas can consume in parallel. PostgreSQL, Redis, and Redpanda state use named volumes.
- **Observability:** API Prometheus counters/histograms are available at `/metrics`; Prometheus DNS discovery scrapes API replicas individually, and the analytics worker exposes processed/dead-letter counters on port `8001`. A Grafana dashboard is provisioned automatically. Prometheus is at `http://localhost:9090`; Grafana is at `http://localhost:3000` (`admin` / `admin` for local development).
- **Orchestration:** `orchestrator/` provides a persistent SQLite-backed dependency-graph engine with parallel branches, entry/exit gates, policy-tag enforcement, approval checkpoints, audit history, bounded retries, fallback handlers, rollback handlers, safe stop, upstream-change invalidation, and reliability metrics. Handlers are an allowlisted adapter interface for agents/tools; the included local handlers are demonstrations, not autonomous production agents.

The Compose topology is a **single-host development/performance-test setup**, not a highly available production deployment. It uses one Redpanda broker with replication factor 1, local default credentials, no API authentication, and no TLS. For production, use a three-or-more-broker Kafka-compatible cluster (replication factor 3), managed PostgreSQL with backups/replicas, TLS, secret-managed credentials, API authentication/rate limits, and capacity-tested Redis/Nginx/API replicas. Budget PostgreSQL connections for every Uvicorn worker and worker pool (Compose defaults allow at most 1 × 8 API connections per API container plus 8 per analytics worker). Treat URLs and workflow context as potentially sensitive; protect database volumes and set retention controls. Don't expose the local Compose ports to untrusted networks.

## Run on Windows

Requirements: Docker Desktop with Compose v2. PowerShell commands below:

```powershell
docker compose up --build -d
docker compose ps
Invoke-RestMethod http://localhost:8080/health/ready
```

Create a link, redirect, and inspect analytics:

```powershell
$link = Invoke-RestMethod -Method Post `
  -Uri http://localhost:8080/api/v1/links `
  -ContentType 'application/json' `
  -Body '{"url":"https://example.com/docs","custom_code":"docs"}'
$link
Invoke-WebRequest -MaximumRedirection 0 $link.short_url
Invoke-RestMethod "http://localhost:8080/api/v1/links/$($link.code)/analytics?days=30"
```

`POST /api/v1/links` accepts `{ "url": "https://...", "custom_code": "optional", "expires_at": "optional RFC3339 timestamp" }`. Responses contain the generated code, destination, creation/expiration times, and short URL. Duplicate custom codes return `409`; invalid input returns `422`.

`GET /{code}` returns `302`, `404` for an unknown code, or `410` after expiry. `GET /api/v1/links/{code}/analytics?days=30` provides total and daily clicks for 1–90 days. Analytics are **eventually visible** after the worker consumes and commits the event. The liveness/readiness probes are `/health/live` and `/health/ready`.

## Scale and benchmark

Bring up several API and analytics consumers; use a machine with enough CPU, memory, and network capacity for the load generator and services:

```powershell
docker compose up --build -d --scale api=4 --scale analytics-worker=3
docker compose ps
docker compose --profile load run --rm -e RATE=10000 -e DURATION=60s k6
```

The k6 scenario creates 500 links during setup, then runs a constant-arrival redirect workload (default 10,000 requests/second for 60 seconds). It checks 302 responses, less than 1% failed requests, p95 latency below 250 ms, and p99 below 500 ms. To change the target, pass a different `RATE`; tune `PREALLOCATED_VUS` and `MAX_VUS` through Compose environment overrides if k6 reports dropped iterations.

Treat **10,000 aggregate redirects/second as an acceptance target to measure**, not a guaranteed capacity claim. A successful run must show a measured `http_reqs` rate at or above the target, zero (or an explicitly explained number of) dropped iterations, thresholds passing, and corresponding click rows persisted after the worker drains the topic. Compare the database count before/after:

```powershell
docker compose exec postgres psql -U shortener -d shortener -c "SELECT count(*) FROM clicks;"
docker compose logs --tail 100 analytics-worker
```

Repeat at several offered rates (for example 1k, 5k, 10k, and 12k RPS), for at least 5 minutes at the target, with realistic cache hit ratios and multiple independently hosted load generators for a serious capacity result. Record CPU/memory, API p95/p99, broker produce latency/errors, consumer lag, PostgreSQL connections/write latency, and the persisted event count. The default 60-second benchmark is a smoke/capacity probe, not a production SLO certification. k6 setup calls and redirect requests share the same API but setup completes before the measured scenario.

### Useful PromQL

```promql
sum(rate(urlshortener_redirects_total{outcome="success"}[1m]))
histogram_quantile(0.95, sum by (le) (rate(urlshortener_redirect_duration_seconds_bucket[5m])))
sum(rate(urlshortener_analytics_publish_failures_total[5m]))
sum(rate(urlshortener_analytics_events_processed_total[5m]))
```

Compose's single broker is not a substitute for multi-zone production monitoring. The workflow engine's success/retry/rollback/MTTR/end-to-end latency counters are available from its CLI metrics command.

## Architecture, scenarios, and governance

```text
User -> Ingress/Active Service -> FastAPI replicas -> Redis cache
                                      | cache miss -> PostgreSQL links
                                      | durable click event -> Kafka/Redpanda
                                      v
                           analytics worker replicas -> PostgreSQL clicks

Requirement -> Architecture -> Implementation -> Tests || Documentation
                                                   -> Security approval
                                                   -> Compliance approval
                                                   -> Change-control approval
                                                   -> Release approval -> Rollout
```

The four approval groups are distinct, sequential, human decisions; a rejected decision prevents downstream release. Each decision records actor, reason, group, and time in the append-only workflow audit table. Scenario context and stage dependency hashes preserve decision lineage. For a real deployment, authorize approvers using the organization's identity provider and role/group memberships; the local CLI accepts a caller-supplied actor string and is a prototype, not an authentication boundary.

The executable scenario gates in `orchestrator/example.py` cover:

| Scenario | Inputs/gate | Orchestrated validation |
|---|---|---|
| Greenfield | Non-empty feature request | Implementation, tests and docs; tests/docs synchronize before security approval |
| Brownfield | Request plus one or more affected modules | Same graph retains affected-module context for review and traceability |
| Ambiguous | Request plus explicit assumptions | Entry gate rejects missing assumptions; accepted assumptions flow into review lineage |

Each scenario is intentionally a local orchestration demo: its handlers return demonstration outputs, not generated production code or actual security/compliance attestations. Replace the adapters with authorized agents/tools, real test/build steps, artifact references, risk evidence, and signed human identity before relying on approvals.

## Kubernetes and rollout feasibility

Kubernetes is a feasible serving platform for the stateless API/worker tiers. The `k8s/` assets provide three API replicas, three analytics workers, CPU requests/limits and HPAs, startup/readiness/liveness probes, PDBs, soft anti-affinity/topology spread, a TLS-ready Ingress shape, NetworkPolicy templates, and restricted application-container settings. Dedicated-node selectors and taints/tolerations are an opt-in overlay; don't enable it until nodes carry the matching label and taint.

The local overlay intentionally includes **single-replica** PostgreSQL, Redis, and Redpanda for a self-contained demo; this is not an HA data plane. The production overlay instead expects externally managed Postgres/Redis/Kafka endpoints and a pre-created Kubernetes Secret named `url-shortener-secrets`. Replace all `registry.example.com`, `replace-with-*`, image, DNS, TLS, and database-secret values before applying it. For production use managed multi-zone database and broker services, tested backup/restore, Redis HA, Kafka replication factor 3+, TLS, secret management, ingress authentication/rate limiting, and tuned resource/connection budgets.

Kubernetes `etcd` is the control-plane key/value store, not an application dependency: do not deploy an etcd StatefulSet for this service. A managed Kubernetes provider owns control-plane/etcd HA and snapshots; self-managed production clusters need an odd-sized control plane (commonly three members), etcd backup/restore drills, encryption-at-rest, and quorum monitoring. Docker Desktop's optional local cluster is single-node and cannot prove node, zone, or control-plane HA.

Blue/green is the default Argo Rollouts strategy: pre-promotion analysis runs, preview Pods are validated, and a human promotes the active Service. `k8s/overlays/canary` offers weighted replica canary steps with pauses, an analysis check, explicit promotion, abort, and undo. The analysis query is a basic cluster-wide redirect-error-rate gate; meaningful production canary analysis needs version/replica labels, actual canary traffic routing, stable-window baselines, alerting, and verified Prometheus/metrics availability. `k8s/overlays/production-hardening/network-policies.yaml` is an in-namespace dependency template: adapt egress selectors/CIDRs for managed service endpoints and ingress selectors for the chosen controller before production use. NetworkPolicy enforcement requires a supporting CNI.

**Argo CD or Flux:** either can reconcile the production Kustomize overlay; choose one control plane, not both for the same objects. Sample `Application` and Flux `GitRepository`/`Kustomization` resources live in `k8s/gitops/`. They contain placeholder repository URLs and require a pushed, access-controlled Git repository. Install Argo Rollouts before syncing the `Rollout` CRD object. Keep production sync/prune changes behind protected branches and CI policy; the sample disables pruning and uses self-heal. Argo Rollouts manual promotion/abort/undo supplies rollout controls; Flux/Argo CD alone do not replace the separate security, compliance, and change-control approvals.

### Run the local Kubernetes end-to-end prototype

Requirements: Docker Desktop Kubernetes enabled in Docker Desktop Settings, a reachable `kubectl` context, and PowerShell. The local commands render/validate manifests without needing the cluster:

```powershell
.\scripts\validate-k8s.ps1
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

Then run the local blue/green example (the script installs the pinned Argo Rollouts controller/plugin, builds `urlshortener:local`, installs local dependencies, pauses for applicable manual promotion gates, creates a URL, follows the redirect status, and verifies durable analytics):

```powershell
.\scripts\k8s-local-e2e.ps1 -Strategy bluegreen
```

To additionally perform a human-approved promotion and rollback/undo probe (the restored revision also pauses for approval):

```powershell
.\scripts\k8s-local-e2e.ps1 -Strategy bluegreen -TestRollback
```

To use the weighted canary overlay:

```powershell
.\scripts\k8s-local-e2e.ps1 -Strategy canary
```

For a non-interactive local smoke only, `-ApproveInitialPromotion` explicitly bypasses the script prompt; do not use it as a production approval mechanism. Inspect `kubectl argo rollouts get rollout url-shortener -n url-shortener`, `kubectl argo rollouts promote ...`, `kubectl argo rollouts abort ...`, and `kubectl argo rollouts undo ...` for manual control. The optional ingress manifest assumes an NGINX ingress controller; the local E2E script uses `kubectl port-forward` and does not require an ingress controller.

### Kubernetes redirect load test

With the local Kubernetes overlay deployed and the active service healthy, forward the service in one PowerShell window:

```powershell
kubectl port-forward service/url-shortener-active 18080:80 --namespace url-shortener
```

Run the same k6 workload from another window to offer 1,000 redirects per second for 60 seconds:

```powershell
k6 run --summary-trend-stats 'avg,min,med,max,p(90),p(95),p(99)' `
  -e BASE_URL=http://127.0.0.1:18080 -e RATE=1000 -e DURATION=60s `
  -e PREALLOCATED_VUS=1000 -e MAX_VUS=3000 load\benchmark.js
kubectl exec postgres-0 --namespace url-shortener -- `
  psql -U shortener -d shortener -Atc "SELECT count(*) FROM clicks"
```

`iterations` and `checks_succeeded` report measured redirect attempts and successful 302 responses; `http_reqs` also includes the 500 setup requests. Confirm the persisted click count increases by the successful redirect count after the analytics worker drains the topic. This local single-node run is a smoke/capacity probe, not a production SLO or HA test. Stop the local application with `kubectl delete namespace url-shortener`. If the Argo Rollouts controller was installed only for this prototype, also remove its pinned install manifest and `argo-rollouts` namespace; do not uninstall a controller shared by other workloads.

### GitOps bootstrap outline

1. Push this repository to a protected Git remote and replace placeholders in `k8s/overlays/production` and `k8s/gitops/*`.
2. Install Argo Rollouts CRDs/controller; provision production dependencies, secrets, ingress, and CNI policies.
3. Install either Argo CD or Flux, then apply its corresponding GitOps resource after configuring repository credentials.
4. Require the security, compliance, and change-control approvals in CI/change management before merging the desired image/manifest revision.
5. Promote the preview/canary only after analysis and human release approval; abort/undo on failed validation. Preserve DB schema compatibility across the rollback window.

## Governed SDLC orchestration

Run the local demo using a persistent SQLite file under `data/`:

```powershell
python -m orchestrator --scenario greenfield start --request "Create durable click analytics"
python -m orchestrator --scenario brownfield start --request "Add expiry behavior" --affected-module app/main.py --affected-module db/init.sql
python -m orchestrator --scenario ambiguous start --request "Support custom aliases" --assumption "Aliases are unique and case-sensitive"
python -m orchestrator status <run-id>
python -m orchestrator audit <run-id>
python -m orchestrator --scenario greenfield approve <run-id> --stage security_approval --actor security-reviewer --reason "Threat review passed" --approve
python -m orchestrator --scenario greenfield approve <run-id> --stage compliance_approval --actor compliance-reviewer --reason "Data controls reviewed" --approve
python -m orchestrator --scenario greenfield approve <run-id> --stage change_control_approval --actor change-manager --reason "Window approved" --approve
python -m orchestrator --scenario greenfield approve <run-id> --stage release --actor release-manager --reason "Release checks passed" --approve
python -m orchestrator --scenario greenfield replan <run-id> --request "Revise the approved change" --actor requester
python -m orchestrator metrics
```

The graph executes requirements → architecture → implementation → tests and documentation in parallel → security → compliance → change control → release approvals. Each stage waits for the appropriate named decision before proceeding. `--change-control frozen` demonstrates a policy safe-stop/failure before execution. `python -m orchestrator stop <run-id> --actor ... --reason ...` requests a safe stop; already-running handlers finish at the current stage boundary and safe stop does not automatically roll back completed work. Re-planning compensates changed completed stages in reverse dependency order before rerunning them; side-effectful non-high-impact handlers should also provide a compensating rollback handler, and other handlers must be idempotent.

Integrate actual agents by registering async handler functions with `WorkflowEngine`; each receives a JSON-compatible context plus dependency outputs and must return a JSON-compatible mapping. Keep tool permissions allowlisted, place authorization and data classification in the adapter/policy layer, and require approvals for irreversible/high-impact actions. The sample graph and local handlers are an executable reference, not a vendor-specific agent runtime.

## Tests

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

The tests cover request validation, DAG/approval behavior, parallel branches, greenfield/brownfield/ambiguous scenario gates, K8s rollout/resource/security controls, re-planning, retry/fallback bounds, rollback, and governance gates. The Kubernetes end-to-end script exercises rollout promotion/undo, API creation/redirect, and durable analytics once a local cluster is enabled.
