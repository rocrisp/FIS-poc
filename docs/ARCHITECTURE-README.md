# FIS dual-site payment demo — how it works

This lab shows **two independent payment sites** (not one stretched Akka cluster) on OpenShift, with **ACM as a third-site arbitrator** for active/standby, and **Kafka + MirrorMaker 2** for async transaction sync.

```text
Active site accepts payments → Kafka events → MM2 mirrors to standby
Arbitrator (hub) tells each site whether it may acceptTraffic
```

---

## Technology map

| Layer | Technology | Role |
|-------|------------|------|
| Hub cluster | OpenShift + **ACM** (`rose-fis`) | Manages SNOs; hosts arbitrator |
| Managed sites | OpenShift **SNO** (`cluster1-fis`, `cluster2-fis`) | Independent payment DCs |
| Connectivity | **Submariner** | Cross-cluster mesh / reachability signal |
| Arbitrator | **Go** service `akka-split-brain-arbitrator` | Active / standby / unreachable decisions |
| Payment app | **Go** `payment-hub-demo` | Polls arbitrator; accepts payments; ledger UI |
| Event bus | **Apache Kafka** (Strimzi) | Local site event log |
| Replication | **MirrorMaker 2** | Async copy of payment topics peer ↔ peer |
| Demo control | Arbitrator **simulation API** + hub GUI | Failover without breaking real mesh |

---

## Big picture

```mermaid
flowchart TB
  subgraph HUB["rose-fis — ACM hub"]
    ACM[ACM / ManagedCluster APIs]
    ARB[Split-brain arbitrator]
    GUI[Hub GUI + simulation]
    ACM --> ARB
    GUI --> ARB
  end

  subgraph C1["cluster1-fis — site A"]
    PH1[payment-hub]
    K1[Kafka]
    MM1[MirrorMaker 2]
    PH1 -->|produce| K1
    PH1 -->|consume local + peer topics| K1
    MM1 -->|mirror out| K1
  end

  subgraph C2["cluster2-fis — site B"]
    PH2[payment-hub]
    K2[Kafka]
    MM2[MirrorMaker 2]
    PH2 -->|produce| K2
    PH2 -->|consume local + peer topics| K2
    MM2 -->|mirror out| K2
  end

  PH1 -->|poll GET /api/v1/state| ARB
  PH2 -->|poll GET /api/v1/state| ARB
  ACM -.->|ManagedCluster + Submariner signals| ARB
  MM1 <-->|Submariner / NLB bootstrap| MM2
  K1 -.->|cluster1-fis.payment-*| K2
  K2 -.->|cluster2-fis.payment-*| K1
```

---

## Components and responsibilities

```mermaid
flowchart LR
  subgraph Signals["Inputs to arbitrator"]
    MC[ManagedCluster reachability]
    SM[Submariner connection health]
    SIM[Simulation override]
  end

  subgraph Decision["Arbitrator"]
    RES[Priority resolver]
    API["/api/v1/state"]
    MC --> RES
    SM --> RES
    SIM --> RES
    RES --> API
  end

  subgraph Sites["Payment hubs"]
    A[Active: acceptTraffic=true]
    S[Standby: refuse new payments]
    API --> A
    API --> S
  end

  subgraph Ledger["Shared event view"]
    KF[Kafka topics]
    A -->|write instructions + lifecycle| KF
    KF -->|MM2| S
    S -->|read replicated events| KF
  end
```

---

## Active / standby decision

Priority order (config): `cluster1-fis` → `cluster2-fis`.

| Condition | cluster1 | cluster2 |
|-----------|----------|----------|
| Healthy mesh, both reachable | **active** | standby |
| Mesh down, both reachable | **active** (priority) | standby |
| cluster1 unreachable | unreachable | **active** |
| cluster2 unreachable | **active** | unreachable |

```mermaid
stateDiagram-v2
  [*] --> Normal: both reachable + mesh OK
  Normal --> MeshDown: Submariner disconnected\n(or sim partition)
  MeshDown --> Normal: mesh restored
  Normal --> Failover: active site unreachable\n(or sim unreachable)
  MeshDown --> Failover: only peer reachable
  Failover --> Normal: clear / recovery

  state Normal {
    [*] --> C1Active
    C1Active: cluster1 active\ncluster2 standby
  }
  state MeshDown {
    [*] --> StillPriority
    StillPriority: cluster1 stays active\nstandby refuses traffic
  }
  state Failover {
    [*] --> PeerActive
    PeerActive: peer acceptTraffic=true\nformer active unreachable
  }
```

Standby is healthy and warm via Kafka.

---

## Payment transaction path

How a real-ish dual-DC payment is mimicked:

```mermaid
sequenceDiagram
  autonumber
  actor User
  participant UI as Active payment-hub UI
  participant Hub as Active payment-hub
  participant Arb as Arbitrator
  participant K1 as Kafka site A
  participant MM2 as MirrorMaker 2
  participant K2 as Kafka site B
  participant Stby as Standby payment-hub

  loop every ~5s
    Hub->>Arb: GET /api/v1/state (X-Cluster-Name)
    Arb-->>Hub: role=active acceptTraffic=true
    Stby->>Arb: GET /api/v1/state
    Arb-->>Stby: role=standby acceptTraffic=false
  end

  User->>UI: Submit payment
  UI->>Hub: POST /api/v1/payments
  Hub->>Hub: acceptTraffic?
  Hub->>K1: payment-instructions
  Hub->>K1: payment-lifecycle (validated)
  Hub-->>UI: 202 accepted
  Hub->>Hub: update local balances

  MM2->>K2: cluster1-fis.payment-*
  Stby->>K2: consume mirrored topics
  Stby->>Stby: show replicated tx + same balances
```

**Balances** are derived on each site from validated lifecycle events (start at 1000 USD per user): debit `from`, credit `to`. Because both sites see the same Kafka stream (local or mirrored), totals stay aligned.

---

## Demo failover (simulation)

Simulation does **not** tear down Submariner; it overrides what the arbitrator reports so apps react by polling.

```mermaid
sequenceDiagram
  participant Demo as Operator / Hub GUI
  participant Arb as Arbitrator
  participant C1 as cluster1 payment-hub
  participant C2 as cluster2 payment-hub

  Note over C1,C2: Normal: C1 active, C2 standby

  Demo->>Arb: PUT /api/v1/simulation<br/>mode=unreachable target=cluster1-fis
  Arb->>Arb: effective: C1 down, mesh down

  C1->>Arb: GET /api/v1/state
  Arb-->>C1: role=unreachable acceptTraffic=false
  C2->>Arb: GET /api/v1/state
  Arb-->>C2: role=active acceptTraffic=true

  Note over C2: New payments accepted on cluster2
  Note over C1: Refuses; may still show ledger from Kafka

  Demo->>Arb: PUT mode=none
  Note over C1,C2: Priority active restored on cluster1
```

| Simulation button | Meaning |
|-------------------|---------|
| Clear (live signals) | Use real ACM / Submariner |
| Submariner mesh down | Partition only; priority active stays |
| Mark cluster N unreachable | Failover to peer |

---

## Kafka topic layout

```mermaid
flowchart LR
  subgraph SiteA["cluster1-fis"]
    L1[payment-instructions]
    L2[payment-lifecycle]
    R2a[cluster2-fis.payment-instructions]
    R2b[cluster2-fis.payment-lifecycle]
  end

  subgraph SiteB["cluster2-fis"]
    L1b[payment-instructions]
    L2b[payment-lifecycle]
    R1a[cluster1-fis.payment-instructions]
    R1b[cluster1-fis.payment-lifecycle]
  end

  L1 -->|MM2 DefaultReplicationPolicy| R1a
  L2 -->|MM2| R1b
  L1b -->|MM2| R2a
  L2b -->|MM2| R2b
```

Topic regexes are anchored (`^payment-instructions$|^payment-lifecycle$`) so bidirectional MM2 does not create feedback loops.

---

## Deploy topology (lab)

```mermaid
flowchart TB
  subgraph Deploy["Deploy order"]
    D1[1. Arbitrator on rose-fis]
    D2[2. Kafka + Strimzi on both SNOs]
    D3[3. Wire MirrorMaker 2 both ways]
    D4[4. payment-hub on both SNOs]
    D1 --> D2 --> D3 --> D4
  end

  subgraph URLs["Browser entry points"]
    U1[Hub arbitrator GUI]
    U2[cluster1 payment-hub]
    U3[cluster2 payment-hub]
  end

  D1 --> U1
  D4 --> U2
  D4 --> U3
```

| Component | Typical URL |
|-----------|-------------|
| Arbitrator | `https://akka-split-brain-arbitrator-open-cluster-management.apps.rose-fis.opdev.io/` |
| Site A | `https://payment-hub-payment-hub.apps.cluster1-fis.opdev.io/` |
| Site B | `https://payment-hub-payment-hub.apps.cluster2-fis.opdev.io/` |

Scripts: `scripts/deploy-arbitrator.sh`, `deploy-kafka-site.sh`, `wire-mirrormaker2.sh`, `deploy-payment-hub.sh`.

---

## What is synced vs not

```mermaid
flowchart TB
  subgraph Synced["Synced / coordinated"]
    R[Roles via arbitrator polls]
    E[Payment events via Kafka + MM2]
    B[Balances derived from same events]
  end

  subgraph NotSynced["Not shared live memory"]
    M[Akka/Pekko actor state]
    P[In-process UI caches until Kafka catch-up]
  end
```

This matches a **dual-datacenter active/standby** payment design: the standby is warm on the event log, not a single stretched cluster.

---

## Repo map

| Path | What it is |
|------|------------|
| `akka-split-brain-arbitrator/` | Hub arbitrator + GUI |
| `payment-hub-demo/` | Per-site payment app + balances/transactions UI |
| `k8s/cluster1-fis/kafka/`, `k8s/cluster2-fis/kafka/` | Kafka + MM2 manifests |
| `docs/arbitrator-api-for-payment-hub.md` | APIs payment hubs call on the arbitrator |
| `docs/EDGE-CASES.md` | Edge-case test checklist |
| `scripts/` | Deploy helpers |

---

## Quick mental model

1. **ACM hub** watches managed clusters + Submariner.  
2. **Arbitrator** publishes who may take traffic.  
3. **Active payment-hub** accepts payments and writes Kafka.  
4. **MM2** copies events to the peer.  
5. **Standby payment-hub** refuses new payments but shows the same transactions and balances.  
6. On **failover** (real or simulated), the peer becomes active and continues from the replicated log.
