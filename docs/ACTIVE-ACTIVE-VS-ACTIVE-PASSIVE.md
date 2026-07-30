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

## What changed in the arbitrator?

Almost nothing about *how it watches the clusters*.  
The change is the **rule** it applies:

| Situation | Active-passive says | Active-active says |
|-----------|---------------------|--------------------|
| Both sites healthy | Cluster1 open, cluster2 closed | **Both open** |
| Sites can’t talk to each other ([mesh down](#mesh)) | Still only one open | **Both stay open** (each is a “home site”; copies may lag) |
| One site is dead | The other opens | The other opens (same idea — “sole active”) |
| Sites can’t reach head office | (weak / stale) | Prefer **cluster1** open; cluster2 closed |

So the main code change is in `datacenter/resolver.go`:  
**“only the winner accepts” → “every healthy site accepts.”**

A few new fields tell the payment apps about that (who’s open, who’s alone, who’s preferred if the hub is gone). The GUI also got a **hub unreachable** button for AA.

What stayed the same: watching ACM/[Submariner](#mesh), storing state, health APIs, traffic log.

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
