# FIS dual-site deploy manifests

## Layout

| Path | Purpose |
|------|---------|
| `acm/` | Example ACM/Hive SNO provisioning (**placeholders only**) |
| `cluster1-fis/kafka/` | SNO Kafka + MM2 (default **active**) |
| `cluster2-fis/kafka/` | SNO Kafka + MM2 (default **standby**) |
| `00-strimzi-install.sh` | Strimzi operator install |

## Related code

- `../akka-split-brain-arbitrator` — ACM-side active/standby status API
- `../payment-hub-demo` — per-site payment app
- `../docs/ARCHITECTURE-README.md` — how the system works
