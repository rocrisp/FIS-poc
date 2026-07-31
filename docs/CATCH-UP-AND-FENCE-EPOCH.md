# Ledger catch-up and fence epoch (safe rule for money)

**Safe rule:** a site that was down or fenced must **not** accept new payments until it has caught up from the log (or proven catch-up for the current fence).

---

## Phase 1 (implemented) — local catch-up gate

In `payment-hub-active-active`:

```text
accept payment  ⟺  arbitrator.acceptTraffic
                 AND ledgerReady
                 AND (home affinity OK  OR  anyPayer)
```

| Behavior | Detail |
|----------|--------|
| Startup | `ledgerReady=false` until Kafka consumer reaches high-watermark on assigned ledger partitions |
| Fenced (`acceptTraffic=false`) | Gate invalidated — must catch up again after return |
| Return to open | `catching_up_after_fence` → refuse with `503 catching_up` until ready |
| Health / UI | Exposes `ledgerReady`, `catchUpReason` |

Sole-active from the hub is **not** enough by itself: the site still waits for `ledgerReady`.

Env: `CATCH_UP_IDLE` (default `2s`) — brief drain window after partitions hit the tip.

---

## Phase 2 (planned) — stronger hub handshake

Hub does not advertise `acceptTraffic=true` until the site reports catch-up for a **fence epoch**.

```text
1. Hub bumps fenceEpoch when site leaves/returns or sole-active flips
2. Hub: eligible=true, acceptTraffic=false, fenceEpoch=N
3. Site rebuilds ledger, then POST caught-up { fenceEpoch: N }
4. Hub: acceptTraffic=true for that site
```

| Piece | Change |
|-------|--------|
| AA arbitrator | Per-site `fenceEpoch`, `caughtUp`; `POST /api/v1/sites/caught-up`; resolver gates `acceptTraffic` |
| AA payment hub | Already has local gate; also report caught-up when ready for epoch N |
| GUI | Show epoch + “awaiting catch-up” |
| Hub-down | Keep local fallback + local gate (sites can’t report to hub) |

Phase 1 covers the money rule even if the hub still says `acceptTraffic=true`. Phase 2 makes the hub’s flag match reality.

See also: [STRETCHED-CLUSTER-SPLIT-BRAIN.md](./STRETCHED-CLUSTER-SPLIT-BRAIN.md).
