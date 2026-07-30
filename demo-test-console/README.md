# Demo test console

Local web UI that lists and runs the FIS dual-site demo checks against live OpenShift routes.

## Run

```bash
go run .
open http://127.0.0.1:8090
```

Click **Run all tests**.

## Cases

1. Arbitrator health  
2. Arbitrator: cluster1-fis is active  
3. Arbitrator: cluster2-fis is standby  
4. Active payment-hub health  
5. Standby payment-hub health  
6. Active accepts POST /payments  
7. Standby refuses POST /payments (`standby_or_fenced`)

## Env

| Variable | Default |
|----------|---------|
| `ARBITRATOR_URL` | hub arbitrator Route |
| `ACTIVE_HUB_URL` | cluster1 payment-hub Route |
| `STANDBY_HUB_URL` | cluster2 payment-hub Route |
| `LISTEN_ADDR` | `:8090` |
