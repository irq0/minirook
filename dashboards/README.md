# Custom Grafana dashboards

Drop Grafana dashboard **`*.json`** files in this directory. They are auto-deployed with
the monitoring stack.

## How it works

`minirook.py`'s `apply_local_grafana_dashboards()` (called from `deploy_monitoring()`) turns
each `*.json` file here into a ConfigMap in the `monitoring` namespace, labeled
`grafana_dashboard=1`. The kube-prometheus-stack Grafana sidecar watches for that label and
imports/updates the dashboards **live** — no Grafana restart required.

## Usage

1. Export a dashboard from Grafana (Share → Export → *Save to file*), or drop in any
   dashboard-model JSON.
2. Put the `.json` file in this folder (filename becomes the ConfigMap name, slugified).
3. Deploy / re-deploy monitoring:
   - as part of the full setup: `uv run python minirook.py setup`
   - or standalone: `uv run python minirook.py monitoring`
4. Open Grafana (`uv run python minirook.py forward-monitoring`) — the dashboard appears
   within a few seconds.

## Notes

- Any valid Grafana dashboard-model JSON works. To pin it into a Grafana folder, add a
  `grafana_dashboard_folder` annotation per the sidecar docs.
- A dashboard's `datasource` should reference the stack's Prometheus (usually name
  `Prometheus` or UID `prometheus`) so panels resolve.
- Editing a file and re-running picks up the change (the ConfigMap is re-applied and the
  sidecar reloads it).
