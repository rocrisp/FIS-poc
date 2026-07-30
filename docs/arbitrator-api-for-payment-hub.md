# Arbitrator APIs for payment hubs

Base URL (lab):

```text
https://akka-split-brain-arbitrator-open-cluster-management.apps.rose-fis.opdev.io
```

Payment hubs set `ARBITRATOR_URL` to this value (no trailing slash). Lab OpenShift Routes use edge TLS; demos typically set `ARBITRATOR_INSECURE_SKIP_VERIFY=true`.

---

## What the payment hub uses today

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/api/v1/state` | Poll active/standby for **this** site |

The hub sends header `X-Cluster-Name: <site>` (e.g. `cluster1-fis` or `cluster2-fis`) and reads:

- `datacenter.role` — `active` \| `standby` \| `unreachable`
- `datacenter.acceptTraffic` — only accept new payments when `true`
- `datacenter.reason` — why that decision was made
- `partition.*` — mesh / active-standby hints
- `simulation.*` — demo override in effect (if any)

Default poll interval: `ARBITRATOR_POLL` (default `5s`).

---

## Read APIs (safe for payment hubs)

### `GET /api/v1/state` (required for traffic fencing)

Returns shared arbitrator state **plus** the role resolved for the caller cluster.

**Headers**

| Header | Required | Description |
|--------|----------|-------------|
| `X-Cluster-Name` | Yes (for correct role) | Managed cluster name of the caller (`cluster1-fis`, `cluster2-fis`) |

**Response `200`**

```json
{
  "version": 0,
  "data": {},
  "lastModified": "",
  "datacenter": {
    "name": "cluster1-fis",
    "role": "active",
    "acceptTraffic": true,
    "reason": "priority",
    "partitionDetected": false,
    "activeDatacenter": "cluster1-fis",
    "standbyDatacenter": "cluster2-fis",
    "simulated": false
  },
  "partition": {
    "detected": false,
    "activeDatacenter": "cluster1-fis",
    "standbyDatacenter": "cluster2-fis"
  },
  "simulation": {
    "mode": "none",
    "note": "using live ACM/Submariner signals"
  }
}
```

**Response headers**

| Header | Description |
|--------|-------------|
| `ETag` | Quoted state version, e.g. `"0"` (used with `PUT /api/v1/state`) |
| `Content-Type` | `application/json` |

**Fields payment hubs should honor**

| Field | Type | Meaning |
|-------|------|---------|
| `datacenter.role` | string | `active`, `standby`, or `unreachable` |
| `datacenter.acceptTraffic` | bool | Gate for accepting new payments |
| `datacenter.reason` | string | e.g. `priority`, `standby`, `simulated_unreachable` |
| `datacenter.partitionDetected` | bool | Effective mesh partition (live or simulated) |
| `partition.activeDatacenter` | string | Site that should accept traffic |
| `partition.standbyDatacenter` | string | Peer site |
| `simulation.mode` | string | `none`, `partition`, or `unreachable` |

**Example**

```bash
curl -sk \
  -H 'X-Cluster-Name: cluster1-fis' \
  "$ARBITRATOR_URL/api/v1/state" | jq .
```

---

### `GET /api/v1/health`

Liveness / process health. Does **not** replace role polling.

**Response `200`**

```json
{
  "status": "ok",
  "submarinerMonitoring": "active",
  "uptime": "1h2m3s",
  "simulationMode": "none"
}
```

```bash
curl -sk "$ARBITRATOR_URL/api/v1/health" | jq .
```

---

### `GET /api/v1/overview`

Hub-wide snapshot of **both** sites (roles, reachability, simulation). Useful for dashboards or ops; payment hubs do not need this for fencing (use `/api/v1/state` with their own name).

**Response `200`**

```json
{
  "sites": [
    {
      "name": "cluster1-fis",
      "role": "active",
      "acceptTraffic": true,
      "reason": "priority",
      "partitionDetected": false,
      "activeDatacenter": "cluster1-fis",
      "standbyDatacenter": "cluster2-fis"
    },
    {
      "name": "cluster2-fis",
      "role": "standby",
      "acceptTraffic": false,
      "reason": "standby",
      "partitionDetected": false,
      "activeDatacenter": "cluster1-fis",
      "standbyDatacenter": "cluster2-fis"
    }
  ],
  "observedSubmarinerConnected": true,
  "observedReachability": {
    "cluster1-fis": true,
    "cluster2-fis": true
  },
  "effectiveSubmarinerConnected": true,
  "simulation": {
    "mode": "none"
  },
  "priority": ["cluster1-fis", "cluster2-fis"]
}
```

```bash
curl -sk "$ARBITRATOR_URL/api/v1/overview" | jq .
```

---

### `GET /api/v1/simulation`

Current demo override only.

**Response `200`**

```json
{
  "mode": "none",
  "updatedAt": "2026-07-30T14:00:00Z",
  "note": "using live ACM/Submariner signals"
}
```

When `mode` is `unreachable`, `target` is the forced-down cluster name.

```bash
curl -sk "$ARBITRATOR_URL/api/v1/simulation" | jq .
```

---

### `GET /api/v1/traffic`

Recent inbound arbitrator API traffic (site polls, simulation changes). Used by the hub GUI log window; optional for payment hubs.

**Response `200`**

```json
{
  "entries": [
    {
      "ts": "2026-07-30T14:22:50.123Z",
      "dir": "in",
      "method": "GET",
      "path": "/api/v1/state",
      "cluster": "cluster1-fis",
      "summary": "→ role=active acceptTraffic=true reason=priority partition=false sim=none"
    }
  ]
}
```

```bash
curl -sk "$ARBITRATOR_URL/api/v1/traffic" | jq .
```

---

## Write APIs (available; not used by payment-hub fencing)

Payment hubs should **not** call these for normal operation. They are for shared state / demo controls.

### `PUT /api/v1/state`

Optimistic-locked write of shared `data` blob.

**Headers**

| Header | Required |
|--------|----------|
| `If-Match` | Yes — quoted ETag / version, e.g. `"0"` |
| `X-Cluster-Name` | Recommended |
| `Content-Type` | `application/json` |

**Body**

```json
{ "data": { "any": "json" } }
```

**Responses**

| Code | Meaning |
|------|---------|
| `200` | Updated; body same shape as `GET /api/v1/state` |
| `400` | Missing/invalid `If-Match` or body |
| `409` | Version conflict (`error: version_conflict`, `currentVersion`) |

---

### `PUT` / `POST` `/api/v1/simulation`

Demo failover without breaking real Submariner.

**Body**

```json
{ "mode": "none|partition|unreachable", "target": "cluster1-fis" }
```

| Mode | Effect |
|------|--------|
| `none` | Clear override; use live ACM/Submariner signals |
| `partition` | Submariner mesh down; both sites reachable; priority active stays |
| `unreachable` | Force `target` unreachable; peer can become active (`target` required) |

**Response `200`**: same shape as `GET /api/v1/overview`.

```bash
# Failover demo: mark cluster1 unreachable
curl -sk -X PUT "$ARBITRATOR_URL/api/v1/simulation" \
  -H 'Content-Type: application/json' \
  -d '{"mode":"unreachable","target":"cluster1-fis"}' | jq .

# Restore live signals
curl -sk -X PUT "$ARBITRATOR_URL/api/v1/simulation" \
  -H 'Content-Type: application/json' \
  -d '{"mode":"none"}' | jq .
```

---

## Roles (not good/bad)

| Role | `acceptTraffic` | Payment hub behavior |
|------|-----------------|----------------------|
| `active` | `true` | Accept payments; publish to local Kafka |
| `standby` | `false` | Refuse new payments; stay warm via MM2 |
| `unreachable` | `false` | Refuse new payments |

---

## Recommended client flow

```text
loop every ARBITRATOR_POLL:
  GET /api/v1/state
    Header: X-Cluster-Name = CLUSTER_NAME
  if datacenter.acceptTraffic:
    accept POST /api/v1/payments locally
  else:
    return standby_or_fenced
```

Optional: call `GET /api/v1/health` for readiness of the arbitrator process only — never treat health alone as permission to take traffic.

---

## UI (not required by payment hubs)

| Path | Description |
|------|-------------|
| `GET /` or `GET /ui` | Hub dashboard (roles + simulation + traffic log) |
| `GET /static/*` | Dashboard assets |

---

## Related payment-hub APIs (local to each site)

These are **not** arbitrator APIs; listed for contrast:

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/health` | Local role cache + cluster id |
| `GET` | `/api/v1/events` | Local + replicated transactions |
| `GET` | `/api/v1/balances` | User balances from Kafka ledger |
| `GET` | `/api/v1/traffic` | Local arbitrator poll log |
| `POST` | `/api/v1/payments` | Submit payment (active only) |
