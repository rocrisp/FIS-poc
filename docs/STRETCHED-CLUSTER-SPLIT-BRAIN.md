# Shared-state stretched cluster active-active — and split brain

This explains the **classic stretched-cluster model** (one logical cluster / shared state across two sites).  
It is **not** what this PoC runs today. Our lab uses two separate apps + hub arbitrator + affinity (see [ACTIVE-ACTIVE.md](./ACTIVE-ACTIVE.md)).

---

## What “shared-state stretched cluster” means

Imagine **one** payment system whose members live in two buildings:

```text
        Site A (cluster1)              Site B (cluster2)
        ┌─────────────┐                ┌─────────────┐
        │  nodes      │◄── network ───►│  nodes      │
        │  (same      │    (mesh)      │  (same      │
        │   cluster)  │                │   cluster)  │
        └─────────────┘                └─────────────┘
                 \                        /
                  \   shared state       /
                   \  (one ledger /     /
                    \  one membership) /
```

Important properties:

| Idea | Meaning |
|------|---------|
| **One cluster** | Nodes in A and B are members of the *same* cluster (e.g. one Akka Cluster) |
| **Shared state** | There is one logical ledger / actor / CRDT / replicated journal — not two independent apps guessing |
| **Active-active** | Clients can write at *either* site; the cluster coordinates so both sides see one consistent story (or a defined conflict rule) |
| **Stretch** | The cluster is “stretched” across a WAN / site link ([Submariner mesh](./ACTIVE-ACTIVE-VS-ACTIVE-PASSIVE.md#mesh) in our lab language) |

Healthy path:

1. Payment hits site A or site B.  
2. The cluster replicates the update to the other site (sync or async, depending on design).  
3. Both sites show the same balances (after replication catches up).  
4. Membership gossip keeps “who is in the cluster” agreed.

---

## What “split brain” means here

**Split brain** = the network between sites breaks (mesh down), but **both sides are still up** and each side still thinks it can serve traffic.

```text
        Site A                         Site B
        ┌─────────────┐     X X X      ┌─────────────┐
        │  still live │◄── cut ───X───►│  still live │
        │  still      │                │  still      │
        │  serving?   │                │  serving?   │
        └─────────────┘                └─────────────┘
              ▲                              ▲
              │                              │
         clients here                   clients here
```

The danger: **two islands**, each with a copy of the shared state, each accepting writes.  
When the link returns, those writes may **conflict** (same account spent twice, different balances).

That is the split-brain problem for shared-state active-active.

---

## What exactly happens on split brain (stretched / shared state)

There is no free lunch. A real system must pick a **policy**. Typical options:

### Option A — Quorum / majority (most common for “one truth”)

| Step | What happens |
|------|----------------|
| 1. Mesh cuts | Cluster detects unreachable members |
| 2. Each side counts members | “Do I still have a majority?” |
| 3a. Majority side | Keeps accepting writes (or becomes the only writer) |
| 3b. Minority side | **Stops** accepting writes (or shuts down / becomes read-only) — *fenced* |
| 4. Mesh returns | Minority rejoins, catches up from majority |

**If neither side has majority** (e.g. 50/50 split with even nodes): both may **refuse writes** until the link returns (availability sacrificed for safety).

This avoids two writers rewriting the same ledger. It is closer to **active-passive during the partition**, even if the product is marketed as active-active when healthy.

### Option B — Both sides keep writing (AP / CRDT / merge later)

| Step | What happens |
|------|----------------|
| 1. Mesh cuts | Both sides stay open |
| 2. Clients pay locally | Each site updates its local replica |
| 3. Mesh returns | System **merges** histories |

Merge needs rules: last-write-wins, CRDTs, manual reconcile, or reject some transactions.  
For **money**, merge is hard — double-spend is not OK. Banks usually prefer Option A or **pin accounts to one site** (affinity).

### Option C — External arbitrator / witness (third site)

| Step | What happens |
|------|----------------|
| 1. Mesh cuts | Both sites ask a **third place** (hub / witness): “Who may write?” |
| 2. Hub answers | e.g. “only site A” or “both with rules” or “site B sole” |
| 3. Loser fences | Stops accepting conflicting writes |

This is the spirit of our **ACM hub arbitrator** — but used as a referee outside a stretched membership.

---

## Healthy AA vs split brain (stretched model)

| Phase | What clients see | Shared state |
|-------|------------------|--------------|
| **Healthy** | Pay at either site (true AA, if designed that way) | One logical ledger; replication keeps both sides aligned |
| **Split brain + quorum** | Only the majority (or survivor) site takes writes | Minority fenced; no divergent money |
| **Split brain + both write** | Both sites take writes | Two histories → conflict / double-spend risk on heal |
| **Split brain + hub referee** | Hub decides who may write | Same goal as quorum: at most one “truth” for conflicts |

---

## How this PoC is different (important)

This lab is **not** a shared-state stretched cluster.

| | Stretched shared-state AA | This PoC (FIS-poc AA) |
|--|---------------------------|------------------------|
| Processes | One cluster membership across sites | Two independent payment apps |
| State | One shared / replicated ledger inside the cluster | Each site has its own in-memory ledger + Kafka copy |
| Sync | Cluster replication / journal | Kafka + MirrorMaker 2 (async) |
| Conflict avoidance | Quorum, CRDT, or fencing | **Payer home affinity** (a–m / n–z) while both open |
| Split brain ([mesh down](./ACTIVE-ACTIVE-VS-ACTIVE-PASSIVE.md#mesh)) | Membership split → quorum or diverge | Arbitrator keeps **both** open as home sites; affinity still applies; sync may lag |
| Hub role | Optional witness | Required referee for reachability / sole-active / hub-down |

So when you press **“Submariner mesh down”** in the AA GUI:

1. Hub still says both sites `acceptTraffic=true` (home-site mode).  
2. Each site still only takes **its** customers (affinity).  
3. MM2 may stop or lag → ledgers can look different until the mesh returns.  
4. You do **not** get classic Akka “minority down / majority continues” membership behavior — there is no stretched membership.

When you press **“Hub unreachable”**:

1. That is **not** mesh split brain.  
2. Sites can’t ask the referee → policy: **cluster1 only**, cluster2 refuses.

---

## Why people avoid stretched shared-state AA over a WAN

- Gossip / membership is sensitive to latency and flaps.  
- A bad split can pause the minority (or the whole system).  
- Money wants **strong** conflict rules; “merge later” is painful.  
- Ops: “is this node in the cluster?” across two OpenShift SNOs is harder than two apps + a hub.

That is why this PoC uses **two sites + arbitrator + affinity + Kafka**, and only *documents* the stretched model for comparison.

---

## Short answers

**How does shared-state stretched AA work when healthy?**  
One cluster spans both sites; writes at either site update the shared state; replication keeps both sides consistent.

**What happens on split brain?**  
The link between sites dies. Without a rule, both sides may write and diverge. Real systems then either fence the minority (quorum), merge later (risky for money), or ask a third-party referee (hub/witness).

**What does this PoC do instead?**  
No stretched membership. Mesh down → both home sites stay open with affinity. Hub down → cluster1 sole writer. Details: [ACTIVE-ACTIVE.md](./ACTIVE-ACTIVE.md).
