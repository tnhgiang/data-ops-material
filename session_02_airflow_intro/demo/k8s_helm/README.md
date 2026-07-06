# Airflow on Kubernetes via Helm — Demo Stack (local Minikube)

Runs the same Airflow version as the lab (2.9.3) on a local Minikube cluster via the official Helm chart, side by side with the Docker Compose stack.

## Scripts

The full flow is packaged as scripts (tested end-to-end on Apple Silicon):

```bash
./scripts/00_check.sh              # pre-flight: tools, RAM, warns if the Compose stack is running
./scripts/01_up.sh                 # minikube start + helm install (waits for pods)
./scripts/02_port_forward.sh       # tunnels: UI 8081 + Flower 5556
./scripts/03_verify.sh             # automated check: health + admin/admin login + Flower
./scripts/04_demo_self_healing.sh  # kill the worker pod → K8s recreates it
./scripts/05_demo_scale.sh 2       # scale workers (rerun with 1 to scale back)
./scripts/99_cleanup.sh            # stop tunnels + minikube stop (--full to delete the cluster)
```

First run of `01_up.sh` takes ~10 minutes (image pulls); later runs ~2-3 minutes thanks to cache. Run `00 → 01 → 02 → 03` once beforehand so images get cached.

## 1. Prerequisites

Docker Desktop, `minikube`, `helm`, `kubectl`:

```bash
brew install minikube helm kubernetes-cli   # macOS
```

Don't run this stack and the lab Compose stack at the same time on a machine with < 12GB RAM for Docker — stop the Compose stack first (`docker compose --profile flower stop`).

## 2. Start Minikube

```bash
minikube start --cpus 4 --memory 8192   # only ~8GB in Docker Desktop → use --memory 6144
kubectl get nodes
```

## 3. Install Airflow with the official Helm chart

```bash
helm repo add apache-airflow https://airflow.apache.org
helm repo update

helm install airflow apache-airflow/airflow \
  --namespace airflow --create-namespace \
  --version 1.15.0 -f values-demo.yaml

# Watch the pods come up
kubectl get pods -n airflow --watch
```

Chart `1.15.0` runs Airflow **2.9.3** by default — the same version as the lab's `docker-compose.yaml`.

## 4. Access the UIs

```bash
# Airflow Webserver — chart default login: admin / admin
kubectl port-forward svc/airflow-webserver 8081:8080 -n airflow

# Flower (separate terminal)
kubectl port-forward svc/airflow-flower 5556:5555 -n airflow
```

| UI | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8081 | `admin` / `admin` |
| Flower | http://localhost:5556 | — |

Ports 8081/5556 are chosen so this stack can run side by side with the Docker Compose stack on 8080/5555.

## 5. Cleanup

```bash
helm uninstall airflow -n airflow
minikube stop        # keep the cluster for a fast start next time
# minikube delete    # remove entirely to free up disk space
```

## 6. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Machine crawls, everything slow | Lab Compose stack and Minikube running at the same time (two Airflow stacks fighting for RAM/CPU) | `docker compose stop` the lab stack first; with < 12GB RAM in Docker Desktop, don't run both |
| Pods stuck `Pending` | Minikube short on CPU/RAM | `minikube delete`, then `start` again with `--cpus 4 --memory 8192` (only ~8GB in Docker Desktop → use `--memory 6144`) |
| `ImagePullBackOff` on `airflow-postgresql-0`, error `manifest unknown` | Bitnami removed old tags from `docker.io/bitnami` (moved to `bitnamilegacy`) | Already fixed in `values-demo.yaml` (`postgresql.image` block) — don't remove it; using another chart version, adjust the tag accordingly |
| `ImagePullBackOff` on other pods | Slow network, images not cached yet | Run the flow once beforehand; or `minikube ssh docker pull apache/airflow:2.9.3` |
| Port-forward drops | Pod restarted | Rerun `./scripts/02_port_forward.sh` |
| Webserver restarts after every `helm upgrade` (killing the tunnel) | Chart generates a random `webserverSecretKey` on each upgrade | Already fixed in `values-demo.yaml` (static key) — don't remove the `webserverSecretKey` block |
| Webserver login fails | Mixing up credentials with the Compose stack | Helm chart default is `admin/admin` (Compose is `airflow/airflow`) |
