# Active-active vs active-passive (simple guide)

Two demos run side by side on the same lab. Same Kafka and [Submariner mesh](#mesh). Different arbitrator + payment app.

| | Active-passive | Active-active |
|--|----------------|---------------|
| Who can take payments? | **One** site | **Both** sites (when healthy) |
| Arbitrator folder | `akka-split-brain-arbitrator/` | `arbitrator-active-active/` |
| Payment app folder | `payment-hub-demo/` | `payment-hub-active-active/` |

---

## The big idea

Think of two bank branches and a head office (the arbitrator).

**Active-passive:** Head office says “only branch A is open.” Branch B waits.

**Active-active:** Head office says “both branches are open.” To avoid double-spending the same account, each customer has a **home branch** (names a–m → cluster1, n–z → cluster2).

That’s the whole story. Everything else is details for failure cases.

---

<a id="mesh"></a>

## What is the “mesh”?

In this demo, **mesh = Submariner** — the private network path between `cluster1-fis` and `cluster2-fis`.

Think of it as the **phone line between the two branches**. It lets the sites talk to each other as if they were on one network. Here it mainly carries:

- **Kafka MirrorMaker 2** traffic (copying payment events between sites)
- Other site-to-site traffic if needed

The arbitrator on the hub also watches Submariner’s connection health. That’s the “is the mesh up?” signal.

**Mesh down is not the same as hub down:**

| Phrase | Meaning |
|--------|---------|
| **Mesh down** | The two *sites* can’t talk to each other (Submariner path broken or simulated). The hub/arbitrator can still be reachable. |
| **Hub unreachable** | A *site* can’t reach the arbitrator (head office). Different failure. |

When the GUI says **“Submariner mesh down”**, it’s pretending the branch-to-branch phone line is cut — not that head office is gone. In active-active, both sites can still take their own customers; they just may not sync until the mesh comes back.

---

## Arbitrator side by side (AP vs AA)

Same job: watch ACM + [Submariner](#mesh), answer “may this site take payments?”  
Different rule in `datacenter/resolver.go`.

| | Active-passive arbitrator | Active-active arbitrator |
|--|---------------------------|--------------------------|
| Folder / deploy name | `akka-split-brain-arbitrator` | `arbitrator-active-active` → `fis-arbitrator-active-active` |
| Lab URL | [AP arbitrator](https://akka-split-brain-arbitrator-open-cluster-management.apps.rose-fis.opdev.io) | [AA arbitrator](https://fis-arbitrator-active-active-open-cluster-management.apps.rose-fis.opdev.io) |
| Decision rule | Pick **one** winner (priority list) | Every **reachable** site may accept |
| Healthy cluster1 | `active`, `acceptTraffic=true` | `active`, `acceptTraffic=true` |
| Healthy cluster2 | `standby`, `acceptTraffic=false` | `active`, `acceptTraffic=true` |
| [Mesh down](#mesh) | Still **one** writer (priority stays) | **Both** stay writers (home sites; sync may lag) |
| One site dead | Peer becomes the only active | Peer becomes **sole active** (same idea, clearer labels) |
| Hub unreachable | No dedicated mode | **New:** cluster1 sole writer; cluster2 standby |
| Conflict control | Built in (only one open) | Not in arbitrator — payment app uses home affinity |

### What each site hears (healthy)

```text
ACTIVE-PASSIVE                         ACTIVE-ACTIVE
─────────────────                      ─────────────────
cluster1: OPEN                         cluster1: OPEN
cluster2: CLOSED (standby)             cluster2: OPEN
```

### Failure buttons (GUI)

| Button | Active-passive | Active-active |
|--------|----------------|---------------|
| Clear (live) | Use real ACM/Submariner | Same |
| Submariner [mesh down](#mesh) | One site stays open | Both stay open as home sites |
| Mark site unreachable | Peer becomes active | Peer becomes sole active |
| Hub unreachable | — (not present) | cluster1 only |

### Extra AA fields (AP does not have these)

| Field | Meaning |
|-------|---------|
| `activePeers` | Which sites may write right now |
| `soleActive` / `soleActiveSite` | Only one writer left |
| `writeMode` | `active-active` or `sole-active` |
| `fallbackActive` | Preferred writer if hubs lose the hub (`cluster1-fis`) |
| `mode: "active-active"` | Marks this as the AA arbitrator |

### What did **not** change

Watching ACM/[Submariner](#mesh), state store, `/api/v1/state` / overview / health / traffic routes, traffic log.

---

## What else makes active-active work?

The arbitrator only answers: **“Is this site allowed to take payments?”**  
It does **not** stop two sites from spending the same account. These pieces do the rest:

1. **`payment-hub-active-active`** (the important one)  
   - Asks the AA arbitrator if it’s open  
   - Sends customers to their **home** site when both are open  
   - If the other site dies → takes **everyone**  
   - If it can’t reach the hub → only cluster1 stays open  

2. **Kafka + MirrorMaker 2** (already there)  
   - Copies payment events between sites so both ledgers can catch up  

3. **[Submariner](#mesh)** (already there)  
   - Site-to-site [mesh](#mesh); also tells the hub if that path looks healthy  

4. **ACM hub (rose-fis)**  
   - Runs both arbitrators  

5. **Deploy scripts**  
   - `deploy-arbitrator-aa.sh` and `deploy-payment-hub-aa.sh`  

Without the AA payment app, flipping the arbitrator to “both open” would be unsafe.

---

## Quick “what happens when…”

| What you do | Active-passive | Active-active |
|-------------|----------------|---------------|
| Everything healthy | Pay only on cluster1 | Pay on **home** site for that customer |
| [Mesh down](#mesh) (button) | Still one writer | Both keep taking **their** customers |
| Mark cluster1 dead | Cluster2 takes over | Cluster2 takes **all** customers |
| Hub unreachable | Risky / stale | Cluster1 only |

---

## Where to look in the code

**Arbitrator — the rule change:**  
`arbitrator-active-active/datacenter/resolver.go`

**Payment app — home site + failover:**  
`payment-hub-active-active/`

More detail (deploy, buttons, URLs): [ACTIVE-ACTIVE.md](./ACTIVE-ACTIVE.md)  

Shared-state stretched cluster + split brain (vs this PoC): [STRETCHED-CLUSTER-SPLIT-BRAIN.md](./STRETCHED-CLUSTER-SPLIT-BRAIN.md)
