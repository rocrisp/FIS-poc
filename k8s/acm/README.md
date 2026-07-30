# ACM / Hive provisioning examples

These manifests are **templates** for provisioning SNO managed clusters (`cluster1-fis`, `cluster2-fis`) on an ACM hub.

**Do not commit real secrets.** Replace all `REPLACE_ME` values (AWS keys, pull secret, SSH key, install-config) locally or via sealed secrets / external secret stores before applying.

For the dual-site payment demo itself you only need the managed clusters already running; start with Kafka + arbitrator + payment-hub under `../cluster1-fis`, `../cluster2-fis`, and `../../scripts/`.
