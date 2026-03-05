import base64
import json
import logging
import os
import platform
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable, Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Protocol

import boto3
import click
import httpx
import kr8s
from botocore.exceptions import ClientError
from kr8s.objects import APIObject, ConfigMap, Deployment, Ingress, Namespace, Secret, Service, objects_from_files
from rich.logging import RichHandler


class K8sResource(Protocol):
    @property
    def raw(self) -> dict[str, Any]: ...
    def exists(self) -> bool: ...
    def patch(self, data: dict[str, Any]) -> None: ...
    def create(self) -> None: ...


class CephCluster(APIObject):
    version = "ceph.rook.io/v1"
    endpoint = "cephclusters"
    kind = "CephCluster"
    plural = "cephclusters"
    singular = "cephcluster"
    namespaced = True


class CephObjectStore(APIObject):
    version = "ceph.rook.io/v1"
    endpoint = "cephobjectstores"
    kind = "CephObjectStore"
    plural = "cephobjectstores"
    singular = "cephobjectstore"
    namespaced = True


class CephObjectStoreUser(APIObject):
    version = "ceph.rook.io/v1"
    endpoint = "cephobjectstoreusers"
    kind = "CephObjectStoreUser"
    plural = "cephobjectstoreusers"
    singular = "cephobjectstoreuser"
    namespaced = True


class StorageClass(APIObject):
    version = "storage.k8s.io/v1"
    endpoint = "storageclasses"
    kind = "StorageClass"
    plural = "storageclasses"
    singular = "storageclass"
    namespaced = False


class Job(APIObject):
    version = "batch/v1"
    endpoint = "jobs"
    kind = "Job"
    plural = "jobs"
    singular = "job"
    namespaced = True

class DaemonSet(APIObject):
    version = "apps/v1"
    endpoint = "daemonsets"
    kind = "DaemonSet"
    plural = "daemonsets"
    singular = "daemonset"
    namespaced = True


class PodMonitor(APIObject):
    version = "monitoring.coreos.com/v1"
    endpoint = "podmonitors"
    kind = "PodMonitor"
    plural = "podmonitors"
    singular = "podmonitor"
    namespaced = True

class ServiceMonitor(APIObject):
    version = "monitoring.coreos.com/v1"
    endpoint = "servicemonitors"
    kind = "ServiceMonitor"
    plural = "servicemonitors"
    singular = "servicemonitor"
    namespaced = True


class TestRun(APIObject):
    version = "k6.io/v1alpha1"
    endpoint = "testruns"
    kind = "TestRun"
    plural = "testruns"
    singular = "testrun"
    namespaced = True


# ==========================================
# Logging
# ==========================================
logging.basicConfig(
    level="INFO", format="%(message)s", datefmt="[%X]", handlers=[RichHandler(rich_tracebacks=True, show_path=False)]
)
log = logging.getLogger("rook-test")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

# ==========================================
# Constants
# ==========================================
KEYSTONE_CLUSTER_URL = "http://keystone-api.openstack.svc.cluster.local:5000"
BARBICAN_CLUSTER_URL = "http://barbican-api.openstack.svc.cluster.local:9311"
RGW_LOCAL_PORT = 8880
KEYSTONE_LOCAL_PORT = 5000
BARBICAN_LOCAL_PORT = 9311
# Kolla publishes separate per-arch tags (NOT a multi-arch manifest list).
# On aarch64 hosts the image is tagged `<release>-debian-bookworm-aarch64`.
_KOLLA_ARCH_SUFFIX = "-aarch64" if platform.machine() in ("arm64", "aarch64") else ""
KEYSTONE_IMAGE = f"quay.io/openstack.kolla/keystone:2025.1-debian-bookworm{_KOLLA_ARCH_SUFFIX}"
BARBICAN_IMAGE = f"quay.io/openstack.kolla/barbican-api:2025.1-debian-bookworm{_KOLLA_ARCH_SUFFIX}"

MONITORING_NAMESPACE = "monitoring"
PROMETHEUS_LOCAL_PORT = 9090
GRAFANA_LOCAL_PORT = 3000
CEPH_DASHBOARD_TAG = "v19.2.0"
CEPH_DASHBOARDS = ("ceph-cluster-advanced", "radosgw-overview", "radosgw-detail")
MTAIL_IMAGE = "ghcr.io/google/mtail:3.0.8"
MTAIL_METRICS_PORT = 3903
MTAIL_PROGS_DIR = Path(__file__).parent / "mtail"

K6_OPERATOR_NAMESPACE = "k6-operator-system"
K6_NAMESPACE = "k6"
K6_DASHBOARD_URL = "https://grafana.com/api/dashboards/19665/revisions/3/download"
PROMETHEUS_RW_URL = (
    f"http://kube-prometheus-stack-prometheus.{MONITORING_NAMESPACE}.svc:9090/api/v1/write"
)
RGW_CLUSTER_URL = "http://rook-ceph-rgw-my-store.rook-ceph.svc:80"
K6_S3_BUCKET = "k6-bench"  # shared by all S3-using k6 targets (rgw-native, rgw-keystone, mixed)


# ==========================================
# Utility Functions
# ==========================================
def run_cmd(cmd: list[str]) -> None:
    log.info(f"Executing: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)


def log_rgw_version() -> None:
    pods = list(kr8s.get("pods", namespace="rook-ceph", label_selector={"app": "rook-ceph-rgw"}))
    if not pods:
        log.warning("No RGW pods found, cannot determine radosgw version.")
        return
    pod_name = pods[0].name
    result = subprocess.run(
        ["kubectl", "exec", "-n", "rook-ceph", pod_name, "--", "radosgw", "--version"],
        capture_output=True,
        text=True,
    )
    version = (result.stdout or result.stderr).strip()
    log.info(f"radosgw on pod '{pod_name}': {version}")


def is_local_image(image_name: str) -> bool:
    log.info(f"Checking if '{image_name}' exists in local Podman...")
    result = subprocess.run(["podman", "image", "exists", image_name])
    return result.returncode == 0


def apply_remote_yaml(url: str) -> None:
    file_name = url.split("/")[-1]
    log.info(f"Downloading and applying {file_name}...")

    resp = httpx.get(url)
    resp.raise_for_status()

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as tf:
        tf.write(resp.text)
        tf_path = tf.name

    try:
        for resource in objects_from_files(tf_path):
            if resource.exists():
                resource.patch(resource.raw)
            else:
                resource.create()
    finally:
        os.remove(tf_path)


def _apply(resource: K8sResource) -> None:
    if resource.exists():
        resource.patch(resource.raw)
    else:
        resource.create()


def _apply_job(job_dict: dict[str, Any]) -> None:
    """Apply a Job, handling immutability: complete -> skip, failed -> delete+recreate."""
    name = job_dict["metadata"]["name"]
    namespace = job_dict["metadata"]["namespace"]

    existing = list(kr8s.get("jobs", name, namespace=namespace))
    if existing:
        conditions = existing[0].raw.get("status", {}).get("conditions", []) or []
        complete = any(c.get("type") == "Complete" and c.get("status") == "True" for c in conditions)
        failed = any(c.get("type") == "Failed" and c.get("status") == "True" for c in conditions)
        if complete:
            log.info(f"Job '{name}' already Complete, skipping.")
            return
        if failed:
            log.info(f"Job '{name}' previously Failed, deleting before re-apply...")
            existing[0].delete()
            _wait_for_condition(
                f"Job '{name}' to be deleted",
                lambda: not list(kr8s.get("jobs", name, namespace=namespace)),
                timeout=30,
                interval=1,
            )
        else:
            log.info(f"Job '{name}' already running, will wait for completion.")
            return

    Job(job_dict).create()


def _wait_for_job(name: str, namespace: str, timeout: int = 300) -> None:
    """Wait for a Job to reach Complete. Raises RuntimeError if it goes to Failed."""
    log.info(f"Waiting for Job '{name}' to Complete...")
    start = time.time()
    while True:
        jobs = list(kr8s.get("jobs", name, namespace=namespace))
        if jobs:
            conditions = jobs[0].raw.get("status", {}).get("conditions", []) or []
            if any(c.get("type") == "Failed" and c.get("status") == "True" for c in conditions):
                pods = list(kr8s.get("pods", namespace=namespace, label_selector={"job-name": name}))
                pod_names = " ".join(p.name for p in pods)
                raise RuntimeError(
                    f"Job '{name}' Failed. Inspect: kubectl logs -n {namespace} {pod_names}"
                )
            if any(c.get("type") == "Complete" and c.get("status") == "True" for c in conditions):
                log.info(f"[bold green]Job '{name}' Complete![/bold green]", extra={"markup": True})
                return
        if time.time() - start > timeout:
            raise TimeoutError(f"Timed out waiting for Job '{name}' to Complete.")
        time.sleep(3)


# ==========================================
# Generic Helpers
# ==========================================
def _wait_for_condition(description: str, check_fn: Callable[[], bool], timeout: int = 300, interval: int = 10) -> None:
    log.info(f"Waiting for {description}...")
    start_time = time.time()
    while True:
        try:
            if check_fn():
                log.info(f"[bold green]{description}: done![/bold green]", extra={"markup": True})
                return
        except Exception as e:
            log.warning(f"Waiting for {description}: {e}")
        if time.time() - start_time > timeout:
            raise TimeoutError(f"Timed out waiting for {description}.")
        time.sleep(interval)


@contextmanager
def port_forward(svc: str, namespace: str, local_port: int, remote_port: int) -> Generator[subprocess.Popen[bytes]]:
    import socket

    log.info(f"Port-forwarding svc/{svc} in {namespace} -> localhost:{local_port}...")
    pf = subprocess.Popen(
        ["kubectl", "port-forward", f"svc/{svc}", f"{local_port}:{remote_port}", "-n", namespace],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    for _ in range(30):
        time.sleep(1)
        try:
            with socket.create_connection(("127.0.0.1", local_port), timeout=1):
                break
        except OSError:
            if pf.poll() is not None:
                raise RuntimeError(f"port-forward to svc/{svc} exited with code {pf.returncode}") from None
    else:
        pf.terminate()
        raise TimeoutError(f"port-forward to svc/{svc} did not become ready in 30s")
    try:
        yield pf
    finally:
        pf.terminate()
        pf.wait()


# ==========================================
# Auto-Detection Functions
# ==========================================
def is_minikube_running() -> bool:
    try:
        result = subprocess.run(["minikube", "status"], capture_output=True)
        return result.returncode == 0
    except Exception:
        return False


def is_rook_deployed() -> bool:
    try:
        pods = list(kr8s.get("pods", namespace="rook-ceph", label_selector={"app": "rook-ceph-operator"}))
        return bool(pods) and all(
            any(
                c.get("type") == "Ready" and c.get("status") == "True"
                for c in p.raw.get("status", {}).get("conditions", [])
            )
            for p in pods
        )
    except Exception:
        return False


def is_ceph_healthy() -> bool:
    try:
        clusters = list(kr8s.get("cephclusters", namespace="rook-ceph"))
        return bool(clusters) and clusters[0].raw.get("status", {}).get("ceph", {}).get("health") == "HEALTH_OK"
    except Exception:
        return False


def is_object_store_ready() -> bool:
    try:
        stores = list(kr8s.get("cephobjectstores", namespace="rook-ceph"))
        return bool(stores) and stores[0].raw.get("status", {}).get("phase") == "Ready"
    except Exception:
        return False


def is_namespace_exists(name: str) -> bool:
    try:
        ns = list(kr8s.get("namespaces", name))
        return bool(ns)
    except Exception:
        return False


def is_object_store_keystone_configured() -> bool:
    try:
        stores = list(kr8s.get("cephobjectstores", namespace="rook-ceph"))
        return bool(stores) and "keystone" in stores[0].raw.get("spec", {}).get("auth", {})
    except Exception:
        return False


def is_mariadb_deployed() -> bool:
    try:
        pods = list(kr8s.get("pods", namespace="openstack", label_selector={"app": "mariadb"}))
        if not pods:
            return False
        return all(
            any(
                c.get("type") == "Ready" and c.get("status") == "True"
                for c in p.raw.get("status", {}).get("conditions", [])
            )
            for p in pods
        )
    except Exception:
        return False


def _deployment_ready(name: str, namespace: str = "openstack") -> bool:
    try:
        deps = list(kr8s.get("deployments", name, namespace=namespace))
        if not deps:
            return False
        status = deps[0].raw.get("status", {})
        return bool(
            status.get("readyReplicas", 0) >= 1
            and status.get("availableReplicas", 0) >= 1
        )
    except Exception:
        return False


def is_keystone_native_deployed() -> bool:
    return _deployment_ready("keystone-api", "openstack")


def is_barbican_native_deployed() -> bool:
    return _deployment_ready("barbican-api", "openstack")


def is_monitoring_deployed() -> bool:
    return _deployment_ready("kube-prometheus-stack-grafana", MONITORING_NAMESPACE)


def is_mtail_deployed() -> bool:
    try:
        ds = list(kr8s.get("daemonsets", "mtail", namespace=MONITORING_NAMESPACE))
        if not ds:
            return False
        status = ds[0].raw.get("status", {})
        ready = status.get("numberReady", 0)
        desired = status.get("desiredNumberScheduled", 0)
        return bool(ready >= 1 and ready == desired)
    except Exception:
        return False


# ==========================================
# Core Infrastructure
# ==========================================
def start_minikube() -> None:
    log.info("Starting Minikube VM according to official Rook dev guidelines...")
    run_cmd(["minikube", "start", "--driver=vfkit", "--memory=16384", "--cpus=4", "--disk-size=40g", "--extra-disks=3"])
    run_cmd(["minikube", "addons", "enable", "ingress"])


def deploy_rook() -> None:
    base_url = "https://raw.githubusercontent.com/rook/rook/release-1.15/deploy/examples"

    apply_remote_yaml(f"{base_url}/crds.yaml")
    apply_remote_yaml(f"{base_url}/common.yaml")
    apply_remote_yaml(f"{base_url}/operator.yaml")

    def _operator_ready() -> bool:
        pods = list(kr8s.get("pods", namespace="rook-ceph", label_selector={"app": "rook-ceph-operator"}))
        return bool(pods) and all(
            any(
                c.get("type") == "Ready" and c.get("status") == "True"
                for c in p.raw.get("status", {}).get("conditions", [])
            )
            for p in pods
        )

    _wait_for_condition("Operator pods to be Ready", _operator_ready, timeout=120, interval=2)
    apply_remote_yaml(f"{base_url}/cluster-test.yaml")


def load_image_to_minikube(image_name: str) -> None:
    tar_filename = "custom-rook.tar"
    log.info(f"Exporting {image_name} from Podman...")
    run_cmd(["podman", "save", "-o", tar_filename, image_name])

    log.info("Loading image into Minikube's registry...")
    run_cmd(["minikube", "image", "load", tar_filename])
    run_cmd(["rm", tar_filename])


def upgrade_rook_operator(image_name: str) -> None:
    log.info(f"Upgrading Rook Operator to use image: [bold cyan]{image_name}[/]", extra={"markup": True})

    [cluster] = list(kr8s.get("cephclusters", namespace="rook-ceph"))
    cluster.patch(
        {
            "spec": {
                "cephVersion": {
                    "allowUnsupported": True,
                    "image": image_name,
                }
            }
        }
    )

    log.info("[bold green]CephCluster updated, waiting for reconciliation...[/]", extra={"markup": True})


# ==========================================
# Object Store
# ==========================================
def deploy_object_store() -> None:
    log.info("Deploying CephObjectStore...")

    _apply(
        ConfigMap(
            {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": "rook-config-override", "namespace": "rook-ceph"},
                "data": {"config": "[client]\nrgw crypt require ssl = false\n\n[global]\ndebug rgw = 20\n"},
            }
        )
    )

    _apply(
        CephObjectStore(
            {
                "apiVersion": "ceph.rook.io/v1",
                "kind": "CephObjectStore",
                "metadata": {"name": "my-store", "namespace": "rook-ceph"},
                "spec": {
                    "metadataPool": {"failureDomain": "osd", "replicated": {"size": 1}},
                    "dataPool": {"failureDomain": "osd", "replicated": {"size": 1}},
                    "gateway": {
                        "port": 80,
                        "instances": 1,
                        "opsLogSidecar": {"resources": {"requests": {}, "limits": {}}},
                    },
                },
            }
        )
    )

    _wait_for_condition("CephObjectStore to become Ready", is_object_store_ready, timeout=300, interval=10)

    _apply(
        CephObjectStoreUser(
            {
                "apiVersion": "ceph.rook.io/v1",
                "kind": "CephObjectStoreUser",
                "metadata": {"name": "irq0", "namespace": "rook-ceph"},
                "spec": {"store": "my-store", "displayName": "Minikube S3 User"},
            }
        )
    )

    _apply(
        Ingress(
            {
                "apiVersion": "networking.k8s.io/v1",
                "kind": "Ingress",
                "metadata": {
                    "name": "rook-ceph-rgw-ingress",
                    "namespace": "rook-ceph",
                    "annotations": {
                        "nginx.ingress.kubernetes.io/proxy-body-size": "0",
                        "nginx.ingress.kubernetes.io/proxy-request-buffering": "off",
                        "nginx.ingress.kubernetes.io/proxy-read-timeout": "600",
                        "nginx.ingress.kubernetes.io/ssl-redirect": "false",
                        "nginx.ingress.kubernetes.io/force-ssl-redirect": "false",
                    },
                },
                "spec": {
                    "ingressClassName": "nginx",
                    "rules": [
                        {
                            "host": "s3.local",
                            "http": {
                                "paths": [
                                    {
                                        "path": "/",
                                        "pathType": "Prefix",
                                        "backend": {
                                            "service": {
                                                "name": "rook-ceph-rgw-my-store",
                                                "port": {"number": 80},
                                            }
                                        },
                                    }
                                ]
                            },
                        }
                    ],
                },
            }
        )
    )

    log.info("[bold green]Object store deployed![/bold green]", extra={"markup": True})


def get_object_store_credentials(store: str, user: str) -> tuple[str, str]:
    secret_name = f"rook-ceph-object-user-{store}-{user}"

    def _secret_ready() -> bool:
        secrets = list(kr8s.get("secrets", secret_name, namespace="rook-ceph"))
        return bool(secrets and secrets[0].raw.get("data", {}).get("AccessKey"))

    _wait_for_condition(f"credentials secret '{secret_name}'", _secret_ready, timeout=120, interval=5)

    [secret] = list(kr8s.get("secrets", secret_name, namespace="rook-ceph"))
    data = secret.raw["data"]
    return (
        base64.b64decode(data["AccessKey"]).decode(),
        base64.b64decode(data["SecretKey"]).decode(),
    )


# ==========================================
# Basic Smoke Tests
# ==========================================
def run_smoke_tests() -> None:
    log.info("Running S3 smoke tests...")
    log_rgw_version()

    access_key, secret_key = get_object_store_credentials("my-store", "irq0")

    s3 = boto3.client(
        "s3",
        endpoint_url=f"http://localhost:{RGW_LOCAL_PORT}",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
    )

    bucket = "smoke-test"
    key = "hello.txt"
    body = b"Hello from minirook smoke test!"

    s3.create_bucket(Bucket=bucket)
    log.info(f"[green]✓ Created bucket '{bucket}'[/]", extra={"markup": True})

    s3.put_object(Bucket=bucket, Key=key, Body=body)
    log.info(f"[green]✓ Uploaded object '{key}'[/]", extra={"markup": True})

    got = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    assert got == body, f"Body mismatch: {got!r}"
    log.info(f"[green]✓ Downloaded and verified '{key}'[/]", extra={"markup": True})

    listed = [o["Key"] for o in s3.list_objects_v2(Bucket=bucket).get("Contents", [])]
    assert key in listed, f"Object not listed: {listed}"
    log.info(f"[green]✓ Listed objects: {listed}[/]", extra={"markup": True})

    s3.delete_object(Bucket=bucket, Key=key)
    s3.delete_bucket(Bucket=bucket)
    log.info(f"[green]✓ Cleaned up bucket '{bucket}'[/]", extra={"markup": True})

    log.info("[bold green]All smoke tests passed![/bold green]", extra={"markup": True})


# ==========================================
# OpenStack Setup
# ==========================================
def setup_openstack_namespace() -> None:
    log.info("Creating 'openstack' namespace...")
    _apply(
        Namespace(
            {
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {"name": "openstack"},
            }
        )
    )

    log.info("Creating 'general' StorageClass (alias for minikube hostpath provisioner)...")
    _apply(
        StorageClass(
            {
                "apiVersion": "storage.k8s.io/v1",
                "kind": "StorageClass",
                "metadata": {"name": "general"},
                "provisioner": "k8s.io/minikube-hostpath",
                "volumeBindingMode": "Immediate",
                "reclaimPolicy": "Delete",
            }
        )
    )

    log.info("Labeling node(s) for openstack scheduling...")
    run_cmd(
        [
            "kubectl",
            "label",
            "node",
            "--all",
            "openstack-control-plane=enabled",
            "--overwrite",
        ]
    )


def _wait_for_app_pods(app: str, namespace: str = "openstack", timeout: int = 300) -> None:
    """Wait for pods matching app=<app> in <namespace> to all be Ready."""

    def _pods_ready() -> bool:
        pods = list(kr8s.get("pods", namespace=namespace, label_selector={"app": app}))
        svc_pods = [
            p
            for p in pods
            if not any(ref.get("kind") == "Job" for ref in p.raw.get("metadata", {}).get("ownerReferences", []))
        ]
        if not svc_pods:
            return False
        return all(
            any(
                c.get("type") == "Ready" and c.get("status") == "True"
                for c in p.raw.get("status", {}).get("conditions", [])
            )
            for p in svc_pods
        )

    _wait_for_condition(f"pods with app={app} to be Ready", _pods_ready, timeout=timeout, interval=5)


def deploy_mariadb() -> None:
    log.info("Deploying MariaDB...")

    probe_cmd = ["mysqladmin", "ping", "-h", "127.0.0.1", "-u", "root", "-ppassword"]
    _apply(
        Deployment(
            {
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {
                    "name": "mariadb",
                    "namespace": "openstack",
                    "labels": {"app": "mariadb"},
                },
                "spec": {
                    "replicas": 1,
                    "selector": {"matchLabels": {"app": "mariadb"}},
                    "template": {
                        "metadata": {"labels": {"app": "mariadb"}},
                        "spec": {
                            "containers": [
                                {
                                    "name": "mariadb",
                                    "image": "mariadb:10.6",
                                    "env": [{"name": "MARIADB_ROOT_PASSWORD", "value": "password"}],
                                    "ports": [{"containerPort": 3306}],
                                    "readinessProbe": {
                                        "exec": {"command": probe_cmd},
                                        "initialDelaySeconds": 5,
                                        "periodSeconds": 5,
                                    },
                                    "livenessProbe": {
                                        "exec": {"command": probe_cmd},
                                        "initialDelaySeconds": 15,
                                        "periodSeconds": 10,
                                    },
                                    "resources": {
                                        "requests": {"memory": "256Mi"},
                                        "limits": {"memory": "512Mi"},
                                    },
                                }
                            ]
                        },
                    },
                },
            }
        )
    )

    _apply(
        Service(
            {
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {
                    "name": "mariadb",
                    "namespace": "openstack",
                },
                "spec": {
                    "type": "ClusterIP",
                    "selector": {"app": "mariadb"},
                    "ports": [{"port": 3306, "targetPort": 3306}],
                },
            }
        )
    )

    _wait_for_condition("MariaDB to be Ready", is_mariadb_deployed, timeout=120, interval=3)


KEYSTONE_CONF = """\
[DEFAULT]
debug = false
notification_format = basic

[database]
connection = mysql+pymysql://root:password@mariadb.openstack.svc.cluster.local/keystone

[token]
provider = fernet
"""


BARBICAN_CONF = """\
[DEFAULT]
host_href = http://barbican-api.openstack.svc.cluster.local:9311
bind_host = 0.0.0.0
bind_port = 9311

[database]
connection = mysql+pymysql://root:password@mariadb.openstack.svc.cluster.local/barbican

[secretstore]
enabled_secretstore_plugins = store_crypto

[crypto]
enabled_crypto_plugins = simple_crypto

[simple_crypto_plugin]
kek = dGhpcnR5X3R3b19ieXRlX2tleWJsYWhibGFoYmxhaGg=

[keystone_authtoken]
www_authenticate_uri = http://keystone-api.openstack.svc.cluster.local:5000
auth_url = http://keystone-api.openstack.svc.cluster.local:5000
memcached_servers =
auth_type = password
project_domain_name = Default
user_domain_name = Default
project_name = admin
username = admin
password = password
service_token_roles_required = true
"""


BARBICAN_PASTE_INI = """\
[composite:main]
use = egg:Paste#urlmap
/: barbican_version
/v1: barbican_api_keystone

[pipeline:barbican_version]
pipeline = cors versionapp

[pipeline:barbican_api_keystone]
pipeline = cors authtoken context apiapp

[app:apiapp]
paste.app_factory = barbican.api.app:create_main_app

[app:versionapp]
paste.app_factory = barbican.api.app:create_version_app

[filter:authtoken]
paste.filter_factory = keystonemiddleware.auth_token:filter_factory

[filter:context]
paste.filter_factory = barbican.api.middleware.context:ContextMiddleware.factory

[filter:cors]
paste.filter_factory = oslo_middleware.cors:filter_factory
oslo_config_project = barbican
"""


def deploy_keystone_native() -> None:
    """Deploy Keystone from the official Kolla multi-arch image.

    Architecture note: Fernet and credential keys are generated by an init
    container in the API Deployment (into emptyDir volumes shared with the
    main container), not by the bootstrap Job. Otherwise the Job's randomly
    generated keys would not be available to the API pod.
    """
    log.info("Deploying Keystone (Kolla image, ARM64-compatible)...")

    _apply(
        ConfigMap(
            {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": "keystone-config", "namespace": "openstack"},
                "data": {"keystone.conf": KEYSTONE_CONF},
            }
        )
    )

    create_db_script = (
        "set -e\n"
        'until mysqladmin ping -h mariadb.openstack.svc.cluster.local -u root -ppassword --silent; do\n'
        '  echo "waiting for mariadb..."; sleep 2;\n'
        'done\n'
        'mysql -h mariadb.openstack.svc.cluster.local -u root -ppassword '
        '-e "CREATE DATABASE IF NOT EXISTS keystone CHARACTER SET utf8"\n'
    )
    bootstrap_script = (
        "set -e\n"
        "keystone-manage fernet_setup --keystone-user root --keystone-group root\n"
        "keystone-manage credential_setup --keystone-user root --keystone-group root\n"
        "keystone-manage db_sync\n"
        "keystone-manage bootstrap "
        "--bootstrap-password=password "
        "--bootstrap-username=admin "
        "--bootstrap-project-name=admin "
        "--bootstrap-role-name=admin "
        "--bootstrap-service-name=keystone "
        "--bootstrap-admin-url=http://keystone-api.openstack.svc.cluster.local:5000 "
        "--bootstrap-public-url=http://keystone-api.openstack.svc.cluster.local:5000 "
        "--bootstrap-internal-url=http://keystone-api.openstack.svc.cluster.local:5000 "
        "--bootstrap-region-id=RegionOne\n"
    )

    _apply_job(
        {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {"name": "keystone-bootstrap", "namespace": "openstack"},
            "spec": {
                "backoffLimit": 5,
                "template": {
                    "metadata": {"labels": {"app": "keystone-bootstrap"}},
                    "spec": {
                        "restartPolicy": "OnFailure",
                        "securityContext": {"runAsUser": 0},
                        "initContainers": [
                            {
                                "name": "create-db",
                                "image": "mariadb:10.6",
                                "command": ["bash", "-c", create_db_script],
                            },
                        ],
                        "containers": [
                            {
                                "name": "bootstrap",
                                "image": KEYSTONE_IMAGE,
                                "imagePullPolicy": "IfNotPresent",
                                "command": ["bash", "-c", bootstrap_script],
                                "volumeMounts": [
                                    {"name": "config", "mountPath": "/etc/keystone/keystone.conf",
                                     "subPath": "keystone.conf"},
                                    {"name": "fernet", "mountPath": "/etc/keystone/fernet-keys"},
                                    {"name": "credential", "mountPath": "/etc/keystone/credential-keys"},
                                ],
                            }
                        ],
                        "volumes": [
                            {"name": "config", "configMap": {"name": "keystone-config"}},
                            {"name": "fernet", "emptyDir": {}},
                            {"name": "credential", "emptyDir": {}},
                        ],
                    },
                },
            },
        }
    )
    _wait_for_job("keystone-bootstrap", "openstack", timeout=300)

    _apply(
        Deployment(
            {
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {
                    "name": "keystone-api",
                    "namespace": "openstack",
                    "labels": {"app": "keystone-api"},
                },
                "spec": {
                    "replicas": 1,
                    "selector": {"matchLabels": {"app": "keystone-api"}},
                    "template": {
                        "metadata": {"labels": {"app": "keystone-api"}},
                        "spec": {
                            "securityContext": {"runAsUser": 0},
                            "initContainers": [
                                {
                                    "name": "fernet-setup",
                                    "image": KEYSTONE_IMAGE,
                                    "imagePullPolicy": "IfNotPresent",
                                    "command": [
                                        "bash", "-c",
                                        "keystone-manage fernet_setup --keystone-user root --keystone-group root && "
                                        "keystone-manage credential_setup --keystone-user root --keystone-group root",
                                    ],
                                    "volumeMounts": [
                                        {"name": "config", "mountPath": "/etc/keystone/keystone.conf",
                                         "subPath": "keystone.conf"},
                                        {"name": "fernet", "mountPath": "/etc/keystone/fernet-keys"},
                                        {"name": "credential", "mountPath": "/etc/keystone/credential-keys"},
                                    ],
                                }
                            ],
                            "containers": [
                                {
                                    "name": "keystone",
                                    "image": KEYSTONE_IMAGE,
                                    "imagePullPolicy": "IfNotPresent",
                                    "command": [
                                        "uwsgi",
                                        "--http", "0.0.0.0:5000",
                                        "--master",
                                        "--processes", "2",
                                        "--enable-threads",
                                        "--module", "keystone.server.wsgi:initialize_public_application()",
                                        "--die-on-term",
                                    ],
                                    "ports": [{"containerPort": 5000}],
                                    "volumeMounts": [
                                        {"name": "config", "mountPath": "/etc/keystone/keystone.conf",
                                         "subPath": "keystone.conf"},
                                        {"name": "fernet", "mountPath": "/etc/keystone/fernet-keys"},
                                        {"name": "credential", "mountPath": "/etc/keystone/credential-keys"},
                                    ],
                                    "readinessProbe": {
                                        "httpGet": {"path": "/v3", "port": 5000},
                                        "initialDelaySeconds": 5,
                                        "periodSeconds": 5,
                                    },
                                    "resources": {
                                        "requests": {"memory": "256Mi"},
                                        "limits": {"memory": "768Mi"},
                                    },
                                }
                            ],
                            "volumes": [
                                {"name": "config", "configMap": {"name": "keystone-config"}},
                                {"name": "fernet", "emptyDir": {}},
                                {"name": "credential", "emptyDir": {}},
                            ],
                        },
                    },
                },
            }
        )
    )

    _apply(
        Service(
            {
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {"name": "keystone-api", "namespace": "openstack"},
                "spec": {
                    "type": "ClusterIP",
                    "selector": {"app": "keystone-api"},
                    "ports": [{"port": 5000, "targetPort": 5000}],
                },
            }
        )
    )

    _wait_for_app_pods("keystone-api", "openstack", timeout=300)
    log.info("[bold green]Keystone deployed.[/bold green]", extra={"markup": True})


def deploy_barbican_native() -> None:
    log.info("Deploying Barbican (Kolla image, ARM64-compatible)...")

    _apply(
        ConfigMap(
            {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": "barbican-config", "namespace": "openstack"},
                "data": {
                    "barbican.conf": BARBICAN_CONF,
                    "barbican-api-paste.ini": BARBICAN_PASTE_INI,
                },
            }
        )
    )

    create_db_script = (
        "set -e\n"
        'until mysqladmin ping -h mariadb.openstack.svc.cluster.local -u root -ppassword --silent; do\n'
        '  echo "waiting for mariadb..."; sleep 2;\n'
        'done\n'
        'mysql -h mariadb.openstack.svc.cluster.local -u root -ppassword '
        '-e "CREATE DATABASE IF NOT EXISTS barbican CHARACTER SET utf8"\n'
    )
    bootstrap_script = (
        "set -e\n"
        "barbican-manage db upgrade\n"
    )

    _apply_job(
        {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {"name": "barbican-bootstrap", "namespace": "openstack"},
            "spec": {
                "backoffLimit": 5,
                "template": {
                    "metadata": {"labels": {"app": "barbican-bootstrap"}},
                    "spec": {
                        "restartPolicy": "OnFailure",
                        "securityContext": {"runAsUser": 0},
                        "initContainers": [
                            {
                                "name": "create-db",
                                "image": "mariadb:10.6",
                                "command": ["bash", "-c", create_db_script],
                            },
                        ],
                        "containers": [
                            {
                                "name": "bootstrap",
                                "image": BARBICAN_IMAGE,
                                "imagePullPolicy": "IfNotPresent",
                                "command": ["bash", "-c", bootstrap_script],
                                "volumeMounts": [
                                    {
                                        "name": "config",
                                        "mountPath": "/etc/barbican/barbican.conf",
                                        "subPath": "barbican.conf",
                                    },
                                    {
                                        "name": "config",
                                        "mountPath": "/etc/barbican/barbican-api-paste.ini",
                                        "subPath": "barbican-api-paste.ini",
                                    },
                                ],
                            }
                        ],
                        "volumes": [
                            {"name": "config", "configMap": {"name": "barbican-config"}},
                        ],
                    },
                },
            },
        }
    )
    _wait_for_job("barbican-bootstrap", "openstack", timeout=300)

    _apply(
        Deployment(
            {
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {
                    "name": "barbican-api",
                    "namespace": "openstack",
                    "labels": {"app": "barbican-api"},
                },
                "spec": {
                    "replicas": 1,
                    "selector": {"matchLabels": {"app": "barbican-api"}},
                    "template": {
                        "metadata": {"labels": {"app": "barbican-api"}},
                        "spec": {
                            "securityContext": {"runAsUser": 0},
                            "containers": [
                                {
                                    "name": "barbican",
                                    "image": BARBICAN_IMAGE,
                                    "imagePullPolicy": "IfNotPresent",
                                    "command": [
                                        "uwsgi",
                                        "--http", "0.0.0.0:9311",
                                        "--master",
                                        "--processes", "2",
                                        "--paste", "config:/etc/barbican/barbican-api-paste.ini",
                                        "--die-on-term",
                                    ],
                                    "ports": [{"containerPort": 9311}],
                                    "volumeMounts": [
                                        {
                                            "name": "config",
                                            "mountPath": "/etc/barbican/barbican.conf",
                                            "subPath": "barbican.conf",
                                        },
                                        {
                                            "name": "config",
                                            "mountPath": "/etc/barbican/barbican-api-paste.ini",
                                            "subPath": "barbican-api-paste.ini",
                                        },
                                    ],
                                    "readinessProbe": {
                                        "httpGet": {"path": "/", "port": 9311},
                                        "initialDelaySeconds": 5,
                                        "periodSeconds": 5,
                                    },
                                    "resources": {
                                        "requests": {"memory": "256Mi"},
                                        "limits": {"memory": "768Mi"},
                                    },
                                }
                            ],
                            "volumes": [
                                {"name": "config", "configMap": {"name": "barbican-config"}},
                            ],
                        },
                    },
                },
            }
        )
    )

    _apply(
        Service(
            {
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {"name": "barbican-api", "namespace": "openstack"},
                "spec": {
                    "type": "ClusterIP",
                    "selector": {"app": "barbican-api"},
                    "ports": [{"port": 9311, "targetPort": 9311}],
                },
            }
        )
    )

    _wait_for_app_pods("barbican-api", "openstack", timeout=300)
    log.info("[bold green]Barbican deployed.[/bold green]", extra={"markup": True})


def deploy_openstack_infra() -> None:
    log.info("Enabling storage-provisioner and default-storageclass addons...")
    run_cmd(["minikube", "addons", "enable", "storage-provisioner"])
    run_cmd(["minikube", "addons", "enable", "default-storageclass"])

    deploy_mariadb()


# ==========================================
# Monitoring (kube-prometheus-stack)
# ==========================================
# Note: this is the only remaining helm-managed component. The
# kube-prometheus-stack chart's images (prometheus, grafana,
# kube-state-metrics, prometheus-operator) are multi-arch, so the ARM64
# concern that drove the openstack-helm removal does not apply here.
def _fetch_ceph_dashboard(name: str) -> str:
    url = (
        f"https://raw.githubusercontent.com/ceph/ceph/{CEPH_DASHBOARD_TAG}"
        f"/monitoring/ceph-mixin/dashboards_out/{name}.json"
    )
    log.info(f"Fetching Ceph dashboard '{name}' from {CEPH_DASHBOARD_TAG}...")
    resp = httpx.get(url, timeout=30)
    resp.raise_for_status()
    return resp.text


def _rook_ceph_service_monitor(name: str, app_label: str, port_name: str) -> ServiceMonitor:
    # Rook v1.15 enables the mgr prometheus module and rolls out rook-ceph-exporter
    # when spec.monitoring.enabled=true, but does NOT create ServiceMonitors —
    # callers must apply them. We hand-roll two minimal ones.
    return ServiceMonitor(
        {
            "apiVersion": "monitoring.coreos.com/v1",
            "kind": "ServiceMonitor",
            "metadata": {"name": name, "namespace": "rook-ceph"},
            "spec": {
                "namespaceSelector": {"matchNames": ["rook-ceph"]},
                "selector": {"matchLabels": {"app": app_label}},
                # No explicit interval — inherits Prometheus' global scrapeInterval (10s).
                "endpoints": [{"port": port_name, "path": "/metrics"}],
            },
        }
    )


def _ceph_dashboard_configmap(name: str, json_str: str) -> ConfigMap:
    return ConfigMap(
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": f"ceph-dashboard-{name}",
                "namespace": MONITORING_NAMESPACE,
                "labels": {"grafana_dashboard": "1"},
            },
            "data": {f"{name}.json": json_str},
        }
    )


def deploy_monitoring() -> None:
    log.info("Deploying kube-prometheus-stack...")

    log.info("Adding prometheus-community Helm repository...")
    run_cmd([
        "helm", "repo", "add", "prometheus-community",
        "https://prometheus-community.github.io/helm-charts",
        "--force-update",
    ])
    run_cmd(["helm", "repo", "update", "prometheus-community"])

    # Ensure namespace exists before applying dashboards into it.
    _apply(
        Namespace(
            {
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {"name": MONITORING_NAMESPACE},
            }
        )
    )

    log.info("Installing kube-prometheus-stack (Prometheus + Grafana + kube-state-metrics)...")
    set_args: list[str] = []
    for key, value in [
        ("alertmanager.enabled", "false"),
        ("nodeExporter.enabled", "false"),
        ("prometheus-node-exporter.enabled", "false"),
        # Discover ServiceMonitors / PodMonitors across all namespaces (so
        # Rook's ServiceMonitor in rook-ceph is picked up).
        ("prometheus.prometheusSpec.serviceMonitorSelectorNilUsesHelmValues", "false"),
        ("prometheus.prometheusSpec.podMonitorSelectorNilUsesHelmValues", "false"),
        # Tight scrape + evaluation cadence for a dev rig — we want fresh
        # data within ~10 seconds of generating it. Targets that don't
        # specify their own interval inherit the global scrapeInterval.
        ("prometheus.prometheusSpec.scrapeInterval", "10s"),
        ("prometheus.prometheusSpec.scrapeTimeout", "10s"),
        ("prometheus.prometheusSpec.evaluationInterval", "10s"),
        # Dial defaults down for a single-node minikube dev rig.
        ("prometheus.prometheusSpec.retention", "12h"),
        ("prometheus.prometheusSpec.resources.requests.memory", "256Mi"),
        ("prometheus.prometheusSpec.resources.limits.memory", "768Mi"),
        # Accept Prometheus remote-write pushes (used by k6 TestRuns).
        ("prometheus.prometheusSpec.enableRemoteWriteReceiver", "true"),
        # Native histograms — k6 emits trend metrics as native histograms when
        # K6_PROMETHEUS_RW_TREND_AS_NATIVE_HISTOGRAM=true; Prometheus must
        # opt-in to ingest+query them.
        ("prometheus.prometheusSpec.enableFeatures[0]", "native-histograms"),
        ("grafana.adminPassword", "admin"),
        ("grafana.resources.requests.memory", "128Mi"),
        ("grafana.resources.limits.memory", "384Mi"),
        # Grafana sidecar that auto-imports labeled ConfigMap dashboards.
        ("grafana.sidecar.dashboards.enabled", "true"),
        ("grafana.sidecar.dashboards.searchNamespace", "ALL"),
        ("grafana.sidecar.dashboards.label", "grafana_dashboard"),
        ("kube-state-metrics.resources.requests.memory", "64Mi"),
        ("kube-state-metrics.resources.limits.memory", "128Mi"),
    ]:
        set_args.extend(["--set", f"{key}={value}"])

    run_cmd([
        "helm", "upgrade", "--install", "kube-prometheus-stack",
        "prometheus-community/kube-prometheus-stack",
        "--namespace", MONITORING_NAMESPACE,
        "--create-namespace",
        "--timeout", "10m",
        *set_args,
    ])

    log.info("Enabling Rook monitoring on CephCluster...")
    [cluster] = list(kr8s.get("cephclusters", namespace="rook-ceph"))
    cluster.patch({"spec": {"monitoring": {
        "enabled": True,
        "exporter": {
            "perfCountersPrioLimit": 2,
            "statsPeriodSeconds": 5,
        },
    }}})

    log.info("Applying ServiceMonitors for rook-ceph-mgr and rook-ceph-exporter...")
    _apply(_rook_ceph_service_monitor("rook-ceph-mgr", "rook-ceph-mgr", "http-metrics"))
    _apply(_rook_ceph_service_monitor(
        "rook-ceph-exporter", "rook-ceph-exporter", "ceph-exporter-http-metrics",
    ))

    log.info("Loading upstream Ceph Grafana dashboards as ConfigMaps...")
    for dash in CEPH_DASHBOARDS:
        body = _fetch_ceph_dashboard(dash)
        _apply(_ceph_dashboard_configmap(dash, body))

    # The chart uses 'app.kubernetes.io/name=grafana' rather than 'app=grafana',
    # so _wait_for_app_pods (keyed on 'app') won't find these pods. Wait on
    # the Deployment readiness instead.
    _wait_for_condition(
        "Grafana Deployment to be Ready",
        lambda: _deployment_ready("kube-prometheus-stack-grafana", MONITORING_NAMESPACE),
        timeout=600,
        interval=5,
    )
    log.info("[bold green]Monitoring stack deployed.[/bold green]", extra={"markup": True})


# ==========================================
# mtail (RGW log → Prometheus metrics)
# ==========================================
def _load_mtail_progs() -> dict[str, str]:
    progs = {p.name: p.read_text() for p in sorted(MTAIL_PROGS_DIR.glob("*.mtail"))}
    if not progs:
        raise RuntimeError(f"No .mtail programs found in {MTAIL_PROGS_DIR}")
    return progs


def apply_mtail_progs_configmap() -> None:
    progs = _load_mtail_progs()
    log.info(f"Applying mtail-progs ConfigMap ({len(progs)} program(s): {', '.join(progs)})")
    _apply(
        Namespace(
            {
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {"name": MONITORING_NAMESPACE},
            }
        )
    )
    _apply(
        ConfigMap(
            {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": "mtail-progs", "namespace": MONITORING_NAMESPACE},
                "data": progs,
            }
        )
    )


def deploy_mtail() -> None:
    log.info("Deploying mtail (RGW beast log -> Prometheus metrics)...")
    apply_mtail_progs_configmap()

    _apply(
        DaemonSet(
            {
                "apiVersion": "apps/v1",
                "kind": "DaemonSet",
                "metadata": {
                    "name": "mtail",
                    "namespace": MONITORING_NAMESPACE,
                    "labels": {"app": "mtail"},
                },
                "spec": {
                    "selector": {"matchLabels": {"app": "mtail"}},
                    "template": {
                        "metadata": {"labels": {"app": "mtail"}},
                        "spec": {
                            "containers": [
                                {
                                    "name": "mtail",
                                    "image": MTAIL_IMAGE,
                                    "imagePullPolicy": "IfNotPresent",
                                    "args": [
                                        "--logtostderr",
                                        "--port", str(MTAIL_METRICS_PORT),
                                        "--progs", "/etc/mtail/progs",
                                        "--logs", "/var/log/containers/rook-ceph-rgw-*.log",
                                        "--logs", "/var/log/containers/keystone-api-*.log",
                                        "--logs", "/var/log/containers/barbican-api-*.log",
                                    ],
                                    "ports": [
                                        {"name": "metrics", "containerPort": MTAIL_METRICS_PORT},
                                    ],
                                    "volumeMounts": [
                                        {"name": "progs", "mountPath": "/etc/mtail/progs", "readOnly": True},
                                        {"name": "varlog", "mountPath": "/var/log/containers", "readOnly": True},
                                        # On minikube w/ docker driver, the symlink chain is:
                                        #   /var/log/containers/<p>.log -> /var/log/pods/<p>/<c>/0.log
                                        #   /var/log/pods/<p>/<c>/0.log -> /var/lib/docker/containers/<id>/<id>-json.log
                                        # mtail's stat() follows both hops, so all three paths must be mounted.
                                        {"name": "varlogpods", "mountPath": "/var/log/pods", "readOnly": True},
                                        {"name": "dockercontainers", "mountPath": "/var/lib/docker/containers", "readOnly": True},
                                    ],
                                    "resources": {
                                        "requests": {"memory": "64Mi", "cpu": "20m"},
                                        "limits": {"memory": "256Mi"},
                                    },
                                    "securityContext": {
                                        "readOnlyRootFilesystem": True,
                                        "runAsNonRoot": False,  # /var/log/pods is root-owned on minikube
                                        "runAsUser": 0,
                                    },
                                }
                            ],
                            "volumes": [
                                {"name": "progs", "configMap": {"name": "mtail-progs"}},
                                {"name": "varlog", "hostPath": {"path": "/var/log/containers", "type": "Directory"}},
                                {"name": "varlogpods", "hostPath": {"path": "/var/log/pods", "type": "Directory"}},
                                {"name": "dockercontainers", "hostPath": {"path": "/var/lib/docker/containers", "type": "Directory"}},
                            ],
                            "tolerations": [
                                {"operator": "Exists"},  # schedule on any node, incl. control-plane (minikube)
                            ],
                        },
                    },
                },
            }
        )
    )

    _apply(
        Service(
            {
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {
                    "name": "mtail",
                    "namespace": MONITORING_NAMESPACE,
                    "labels": {"app": "mtail"},
                },
                "spec": {
                    "type": "ClusterIP",
                    "selector": {"app": "mtail"},
                    "ports": [
                        {"name": "metrics", "port": MTAIL_METRICS_PORT, "targetPort": MTAIL_METRICS_PORT},
                    ],
                },
            }
        )
    )

    _apply(
        PodMonitor(
            {
                "apiVersion": "monitoring.coreos.com/v1",
                "kind": "PodMonitor",
                "metadata": {
                    "name": "mtail",
                    "namespace": MONITORING_NAMESPACE,
                    "labels": {"app": "mtail"},
                },
                "spec": {
                    "namespaceSelector": {"matchNames": [MONITORING_NAMESPACE]},
                    "selector": {"matchLabels": {"app": "mtail"}},
                    "podMetricsEndpoints": [
                        # No explicit interval — inherits Prometheus' global
                        # scrapeInterval (10s, set in deploy_monitoring).
                        {"port": "metrics"},
                    ],
                },
            }
        )
    )

    _wait_for_condition("mtail DaemonSet to be Ready", is_mtail_deployed, timeout=300, interval=5)
    log.info("[bold green]mtail deployed.[/bold green]", extra={"markup": True})

# k6 Load Testing
# ==========================================
def is_k6_operator_deployed() -> bool:
    return _deployment_ready("k6-operator-controller-manager", K6_OPERATOR_NAMESPACE)


def _fetch_k6_dashboard() -> str:
    log.info(f"Fetching k6 Grafana dashboard from {K6_DASHBOARD_URL}...")
    resp = httpx.get(K6_DASHBOARD_URL, timeout=30, follow_redirects=True)
    resp.raise_for_status()
    return resp.text


def _k6_dashboard_configmap(json_str: str) -> ConfigMap:
    return ConfigMap(
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "k6-grafana-dashboard",
                "namespace": MONITORING_NAMESPACE,
                "labels": {"grafana_dashboard": "1"},
            },
            "data": {"k6-prometheus.json": json_str},
        }
    )


def install_k6_operator() -> None:
    log.info("Installing k6-operator...")

    log.info("Adding grafana Helm repository...")
    run_cmd([
        "helm", "repo", "add", "grafana",
        "https://grafana.github.io/helm-charts",
        "--force-update",
    ])
    run_cmd(["helm", "repo", "update", "grafana"])

    # Helm's --create-namespace refuses to adopt a namespace it didn't create,
    # so leave K6_OPERATOR_NAMESPACE for Helm. K6_NAMESPACE holds workload
    # resources only, so we can manage it ourselves.
    _apply(
        Namespace(
            {
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {"name": K6_NAMESPACE},
            }
        )
    )

    log.info(f"Installing k6-operator into {K6_OPERATOR_NAMESPACE}...")
    run_cmd([
        "helm", "upgrade", "--install", "k6-operator",
        "grafana/k6-operator",
        "--namespace", K6_OPERATOR_NAMESPACE,
        "--create-namespace",
        "--timeout", "5m",
    ])

    _wait_for_condition(
        "k6-operator Deployment to be Ready",
        lambda: _deployment_ready("k6-operator-controller-manager", K6_OPERATOR_NAMESPACE),
        timeout=300,
        interval=5,
    )

    log.info("Importing k6 Grafana dashboard...")
    try:
        _apply(_k6_dashboard_configmap(_fetch_k6_dashboard()))
    except Exception as e:
        # Dashboard import is a nice-to-have; don't block install on grafana.com fetch failure.
        log.warning(f"Could not import k6 Grafana dashboard: {e}")

    log.info("[bold green]k6-operator installed.[/bold green]", extra={"markup": True})


def _k6_script_dir() -> str:
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "k6")


def _load_k6_scripts() -> dict[str, str]:
    """Read every .js file under ./k6/ into a {filename: contents} map for a ConfigMap."""
    scripts_dir = _k6_script_dir()
    out: dict[str, str] = {}
    for fn in os.listdir(scripts_dir):
        if fn.endswith(".js"):
            with open(os.path.join(scripts_dir, fn)) as f:
                out[fn] = f.read()
    if not out:
        raise RuntimeError(f"No .js files found under {scripts_dir}")
    return out


def _ensure_k6_namespace() -> None:
    _apply(
        Namespace(
            {
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {"name": K6_NAMESPACE},
            }
        )
    )


def _apply_k6_scripts_configmap(name: str) -> None:
    _apply(
        ConfigMap(
            {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": name, "namespace": K6_NAMESPACE},
                "data": _load_k6_scripts(),
            }
        )
    )


def _testrun_phase(name: str) -> str:
    runs = list(kr8s.get("testruns", name, namespace=K6_NAMESPACE))
    if not runs:
        return ""
    return str(runs[0].raw.get("status", {}).get("stage", ""))


_VERDICT_BEGIN = "===MINIROOK_VERDICT==="
_VERDICT_END = "===END_MINIROOK_VERDICT==="


def _stream_k6_logs(testrun_name: str) -> str:
    """Stream runner logs to stdout AND return the captured text so callers
    can parse the MINIROOK_VERDICT block. Returns "" on error."""
    log.info(f"Runner logs for TestRun '{testrun_name}':")
    try:
        proc = subprocess.run(
            ["kubectl", "logs", "-n", K6_NAMESPACE, "-l", f"k6_cr={testrun_name}", "--tail=-1"],
            check=False,
            capture_output=True,
            text=True,
        )
    except Exception as e:
        log.warning(f"Failed to fetch k6 logs: {e}")
        return ""
    # Tee to stdout so the user still sees the full run output.
    if proc.stdout:
        print(proc.stdout, end="")
    if proc.stderr:
        print(proc.stderr, end="", file=sys.stderr)
    return proc.stdout or ""


def _parse_k6_verdict(logs: str) -> dict[str, Any] | None:
    """Extract the JSON verdict emitted by mixed.js handleSummary().
    Returns None if no verdict block is present (e.g. older script, or
    target other than 'mixed')."""
    begin = logs.rfind(_VERDICT_BEGIN)
    if begin < 0:
        return None
    end = logs.find(_VERDICT_END, begin)
    if end < 0:
        return None
    payload = logs[begin + len(_VERDICT_BEGIN) : end].strip()
    try:
        return json.loads(payload)
    except json.JSONDecodeError as e:
        log.warning(f"Could not parse k6 verdict block: {e}")
        return None


_K6_RUN_TARGETS = ("rgw-native", "rgw-keystone", "barbican", "mixed")


def _fetch_openstack_creds(need_ec2: bool) -> tuple[str | None, str | None, str | None]:
    """Open keystone+barbican port-forwards once and pull whatever creds the
    caller needs. Returns (ec2_access, ec2_secret, kms_key); ec2_* are None
    when need_ec2=False."""
    with (
        port_forward("keystone-api", "openstack", KEYSTONE_LOCAL_PORT, 5000),
        port_forward("barbican-api", "openstack", BARBICAN_LOCAL_PORT, 9311),
    ):
        keystone_url = f"http://localhost:{KEYSTONE_LOCAL_PORT}"
        barbican_url = f"http://localhost:{BARBICAN_LOCAL_PORT}"
        access, secret = _get_ec2_credentials(keystone_url) if need_ec2 else (None, None)
        kms_key = _get_barbican_kms_key(keystone_url, barbican_url)
    return access, secret, kms_key


def _mixed_bucket_names(count: int) -> list[str]:
    """The canonical naming convention shared by prepare/run/teardown."""
    if count < 1:
        raise ValueError(f"buckets must be >= 1 (got {count})")
    if count == 1:
        return [K6_S3_BUCKET]
    return [f"{K6_S3_BUCKET}-{i}" for i in range(count)]


def _setup_mixed_workload(
    count: int, sse_mode: str
) -> tuple[str, str, list[tuple[str, str | None]]]:
    """Create N buckets in RGW (optionally with per-bucket Barbican-backed SSE-KMS).

    Opens keystone, barbican, and RGW port-forwards for the duration of the
    setup. For each bucket creates a fresh Barbican secret iff sse_mode is
    'request' or 'bucket'; for 'bucket' also calls PutBucketEncryption so RGW
    encrypts all writes server-side.

    Returns (ec2_access, ec2_secret, [(bucket_name, kms_key_uuid_or_None), ...]).
    """
    if sse_mode not in ("none", "request", "bucket"):
        raise ValueError(f"Invalid sse_mode '{sse_mode}'")
    bucket_names = _mixed_bucket_names(count)

    with (
        port_forward("keystone-api", "openstack", KEYSTONE_LOCAL_PORT, 5000),
        port_forward("barbican-api", "openstack", BARBICAN_LOCAL_PORT, 9311),
        port_forward("rook-ceph-rgw-my-store", "rook-ceph", RGW_LOCAL_PORT, 80),
    ):
        keystone_url = f"http://localhost:{KEYSTONE_LOCAL_PORT}"
        barbican_url = f"http://localhost:{BARBICAN_LOCAL_PORT}"
        ec2_access, ec2_secret = _get_ec2_credentials(keystone_url)

        token: str | None = None
        rgwcrypt_user_id: str | None = None
        if sse_mode != "none":
            token = _ks_token(keystone_url, "admin", "password", "admin")
            # Resolve once — every Barbican secret we create needs an ACL
            # grant to this user so RGW can read the key at request time.
            r = httpx.get(
                f"{keystone_url}/v3/users",
                headers={"X-Auth-Token": token},
                params={"name": "rgwcrypt"},
            )
            r.raise_for_status()
            users = r.json()["users"]
            if not users:
                raise RuntimeError(
                    "Keystone user 'rgwcrypt' not found — was setup_keystone run?"
                )
            rgwcrypt_user_id = users[0]["id"]

        s3 = boto3.client(
            "s3",
            endpoint_url=f"http://localhost:{RGW_LOCAL_PORT}",
            aws_access_key_id=ec2_access,
            aws_secret_access_key=ec2_secret,
            region_name="us-east-1",
        )

        log.info(f"Preparing {count} bucket(s) with sse_mode={sse_mode}")
        bucket_specs: list[tuple[str, str | None]] = []
        for i, name in enumerate(bucket_names):
            try:
                s3.create_bucket(Bucket=name)
                log.info(f"  Created bucket '{name}'")
            except ClientError as e:
                code = e.response.get("Error", {}).get("Code", "")
                if code not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                    raise
                log.info(f"  Reusing existing bucket '{name}'")

            # In `bucket` mode, reuse the KMS UUID from the bucket's existing
            # encryption config if present — that's the whole point of running
            # `k6 prepare-buckets` once and then many `k6 run mixed`s. Falls
            # back to creating a fresh Barbican secret only when missing.
            kms_id: str | None = None
            if sse_mode == "bucket":
                kms_id = _get_existing_bucket_kms(s3, name)
                if kms_id:
                    log.info(f"  Reusing existing SSE-KMS key {kms_id} on '{name}'")
            if sse_mode != "none" and kms_id is None:
                assert token is not None and rgwcrypt_user_id is not None
                secret_name = f"{name}-kms-{int(time.time())}-{i}"
                kms_id = _create_barbican_secret(
                    barbican_url, token, secret_name, rgwcrypt_user_id
                )
                log.info(f"  Created Barbican secret {kms_id} for bucket '{name}'")
            if sse_mode == "bucket" and kms_id:
                s3.put_bucket_encryption(
                    Bucket=name,
                    ServerSideEncryptionConfiguration={
                        "Rules": [
                            {
                                "ApplyServerSideEncryptionByDefault": {
                                    "SSEAlgorithm": "aws:kms",
                                    "KMSMasterKeyID": kms_id,
                                },
                                "BucketKeyEnabled": True,
                            }
                        ]
                    },
                )
                log.info(f"  Default SSE-KMS set on '{name}'")
            bucket_specs.append((name, kms_id))

        return ec2_access, ec2_secret, bucket_specs


def _get_existing_bucket_kms(s3: Any, bucket: str) -> str | None:
    """Return the KMSMasterKeyID currently set on the bucket's default
    encryption config, or None if no encryption is configured."""
    try:
        cfg = s3.get_bucket_encryption(Bucket=bucket)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("ServerSideEncryptionConfigurationNotFoundError", "NoSuchBucket"):
            return None
        raise
    rules = cfg.get("ServerSideEncryptionConfiguration", {}).get("Rules", [])
    for rule in rules:
        default = rule.get("ApplyServerSideEncryptionByDefault", {})
        if default.get("SSEAlgorithm") == "aws:kms":
            kid = default.get("KMSMasterKeyID")
            if kid:
                return str(kid)
    return None


def _teardown_mixed_workload(count: int, purge_secrets: bool) -> None:
    """Delete the k6-bench bucket(s) and (optionally) the Barbican secrets
    referenced by their default-encryption config."""
    bucket_names = _mixed_bucket_names(count)
    with (
        port_forward("keystone-api", "openstack", KEYSTONE_LOCAL_PORT, 5000),
        port_forward("barbican-api", "openstack", BARBICAN_LOCAL_PORT, 9311),
        port_forward("rook-ceph-rgw-my-store", "rook-ceph", RGW_LOCAL_PORT, 80),
    ):
        keystone_url = f"http://localhost:{KEYSTONE_LOCAL_PORT}"
        barbican_url = f"http://localhost:{BARBICAN_LOCAL_PORT}"
        ec2_access, ec2_secret = _get_ec2_credentials(keystone_url)
        s3 = boto3.client(
            "s3",
            endpoint_url=f"http://localhost:{RGW_LOCAL_PORT}",
            aws_access_key_id=ec2_access,
            aws_secret_access_key=ec2_secret,
            region_name="us-east-1",
        )

        token: str | None = None
        if purge_secrets:
            token = _ks_token(keystone_url, "admin", "password", "admin")

        log.info(f"Tearing down {len(bucket_names)} bucket(s); purge_secrets={purge_secrets}")
        for name in bucket_names:
            kms_id: str | None = None
            if purge_secrets:
                kms_id = _get_existing_bucket_kms(s3, name)

            deleted_objects = 0
            try:
                paginator = s3.get_paginator("list_objects_v2")
                for page in paginator.paginate(Bucket=name):
                    objs = page.get("Contents") or []
                    if not objs:
                        continue
                    s3.delete_objects(
                        Bucket=name,
                        Delete={"Objects": [{"Key": o["Key"]} for o in objs], "Quiet": True},
                    )
                    deleted_objects += len(objs)
                s3.delete_bucket(Bucket=name)
                log.info(f"  Deleted bucket '{name}' ({deleted_objects} objects removed)")
            except ClientError as e:
                code = e.response.get("Error", {}).get("Code", "")
                if code == "NoSuchBucket":
                    log.info(f"  Bucket '{name}' already absent")
                else:
                    log.warning(f"  Failed to delete bucket '{name}': {e}")

            if purge_secrets and kms_id and token:
                try:
                    r = httpx.delete(
                        f"{barbican_url}/v1/secrets/{kms_id}",
                        headers={"X-Auth-Token": token},
                    )
                    r.raise_for_status()
                    log.info(f"  Deleted Barbican secret {kms_id}")
                except httpx.HTTPError as e:
                    log.warning(f"  Failed to delete Barbican secret {kms_id}: {e}")
# Mirror of k6/mixed.js _scenarioDefs keys plus the special-case 'constant'.
# Keep in sync — drift surfaces as confusing 'Unknown SCENARIO' errors
# (CLI-side here, runtime-side in mixed.js).
_MIXED_SCENARIOS = (
    "testing",
    "constant",
    "stress_minikube",
    "demo",
    "breakpoint",
    "stress_workstation",
    "stress_cloud",
)


def run_k6_scenario(
    target: str,
    vus: int,
    duration: str,
    keep: bool = False,
    parallelism: int = 1,
    weights: str | None = None,
    preseed: str | None = None,
    preseed_list_max: int | None = None,
    object_size: int | None = None,
    scenario: str | None = None,
    rate: int | None = None,
    buckets: int = 1,
    sse_mode: str | None = None,
    skip_setup: bool = False,
) -> None:
    if scenario and scenario not in _MIXED_SCENARIOS:
        raise ValueError(f"Unknown SCENARIO '{scenario}'; choose from {_MIXED_SCENARIOS}")
    if rate is not None:
        if rate <= 0:
            raise ValueError("--rate must be > 0")
        if scenario is None:
            scenario = "constant"  # --rate implies --scenario constant
        elif scenario != "constant":
            raise ValueError(
                f"--rate is only valid with --scenario constant (got --scenario {scenario})"
            )
    if scenario == "constant" and rate is None:
        raise ValueError("--scenario constant requires --rate N (N > 0)")

    _ensure_k6_namespace()

    script_cm_name = "k6-scripts"
    _apply_k6_scripts_configmap(script_cm_name)

    # NOTE: env values are inlined (not secretKeyRef'd). The k6-operator
    # templates runner env into `-e KEY="${value}"` CLI flags at TestRun
    # creation time and reads only the `.value` field — so valueFrom entries
    # render as `-e KEY=""`, which silently overrides the container env that
    # secretKeyRef does inject. Inlining is the documented workaround.
    env_vars: list[dict[str, Any]] = [
        {"name": "K6_PROMETHEUS_RW_SERVER_URL", "value": PROMETHEUS_RW_URL},
        # Emit trend metrics as Prometheus native histograms instead of
        # pre-aggregated p95/p99/etc series. Requires Prometheus to be started
        # with --enable-feature=native-histograms (set in deploy_monitoring).
        {"name": "K6_PROMETHEUS_RW_TREND_AS_NATIVE_HISTOGRAM", "value": "true"},
    ]
    script_file: str

    if target == "rgw-native":
        script_file = "s3.js"
        access, secret = get_object_store_credentials("my-store", "irq0")
        env_vars += [
            {"name": "S3_ENDPOINT", "value": RGW_CLUSTER_URL},
            {"name": "AWS_ACCESS_KEY_ID", "value": access},
            {"name": "AWS_SECRET_ACCESS_KEY", "value": secret},
            {"name": "S3_BUCKET", "value": K6_S3_BUCKET},
        ]
    elif target == "rgw-keystone":
        script_file = "s3.js"
        access, secret, kms_key = _fetch_openstack_creds(need_ec2=True)
        env_vars += [
            {"name": "S3_ENDPOINT", "value": RGW_CLUSTER_URL},
            {"name": "AWS_ACCESS_KEY_ID", "value": access},
            {"name": "AWS_SECRET_ACCESS_KEY", "value": secret},
            {"name": "S3_BUCKET", "value": K6_S3_BUCKET},
        ]
        if kms_key:
            env_vars.append({"name": "KMS_KEY_ID", "value": kms_key})
    elif target == "barbican":
        script_file = "barbican.js"
        _, _, kms_key = _fetch_openstack_creds(need_ec2=False)
        env_vars += [
            {"name": "KEYSTONE_URL", "value": KEYSTONE_CLUSTER_URL},
            {"name": "BARBICAN_URL", "value": BARBICAN_CLUSTER_URL},
            {"name": "OS_USERNAME", "value": "admin"},
            {"name": "OS_PROJECT", "value": "admin"},
            {"name": "OS_PASSWORD", "value": "password"},
        ]
        if kms_key:
            env_vars.append({"name": "KMS_KEY_ID", "value": kms_key})
    else:  # mixed
        script_file = "mixed.js"
        # Smart default for --sse: bucket-level when Keystone+Barbican are
        # available (matches ceph-rgw-benchmarking's prepare.py pattern),
        # else none.
        if sse_mode is None:
            sse_mode = (
                "bucket"
                if is_keystone_native_deployed() and is_barbican_native_deployed()
                else "none"
            )
        if skip_setup:
            # Trust that `k6 prepare-buckets` already created the buckets and,
            # in 'bucket' mode, set their default encryption server-side. We
            # still need EC2 creds, but we don't touch Barbican or RGW state.
            if sse_mode == "request":
                raise click.ClickException(
                    "--skip-setup is incompatible with --sse request "
                    "(per-PUT SSE headers need per-bucket KMS UUIDs that aren't "
                    "discoverable from a running cluster — re-run without --skip-setup)."
                )
            access, secret, _kms = _fetch_openstack_creds(need_ec2=True)
            assert access is not None and secret is not None
            bucket_names = _mixed_bucket_names(buckets)
            log.info(f"--skip-setup: reusing {len(bucket_names)} existing bucket(s): {bucket_names}")
            env_vars += [
                {"name": "S3_ENDPOINT", "value": RGW_CLUSTER_URL},
                {"name": "AWS_ACCESS_KEY_ID", "value": access},
                {"name": "AWS_SECRET_ACCESS_KEY", "value": secret},
                {"name": "S3_BUCKETS", "value": ",".join(bucket_names)},
            ]
        else:
            access, secret, bucket_specs = _setup_mixed_workload(buckets, sse_mode)
            bucket_names = [b for b, _ in bucket_specs]
            env_vars += [
                {"name": "S3_ENDPOINT", "value": RGW_CLUSTER_URL},
                {"name": "AWS_ACCESS_KEY_ID", "value": access},
                {"name": "AWS_SECRET_ACCESS_KEY", "value": secret},
                {"name": "S3_BUCKETS", "value": ",".join(bucket_names)},
            ]
            if sse_mode == "request":
                kms_csv = ",".join(k or "" for _, k in bucket_specs)
                env_vars.append({"name": "S3_BUCKET_KMS_KEYS", "value": kms_csv})
        if weights:
            env_vars.append({"name": "MIXED_WEIGHTS", "value": weights})
        if preseed:
            env_vars.append({"name": "PRESEED", "value": preseed})
        if preseed_list_max is not None:
            env_vars.append({"name": "PRESEED_LIST_MAX", "value": str(preseed_list_max)})
        if object_size is not None:
            env_vars.append({"name": "OBJECT_SIZE_BYTES", "value": str(object_size)})
        if scenario:
            env_vars.append({"name": "SCENARIO", "value": scenario})
        if scenario == "constant":
            env_vars.append({"name": "CONSTANT_RATE", "value": str(rate)})
            env_vars.append({"name": "CONSTANT_DURATION", "value": duration})

    run_name = f"{target}-{int(time.time())}"
    # When SCENARIO drives the load shape, ignore the CLI --vus/--duration
    # (they would conflict with the scenarios block in mixed.js).
    if scenario:
        arguments = f"-o experimental-prometheus-rw --tag testid={target}"
    else:
        arguments = (
            f"-o experimental-prometheus-rw "
            f"--tag testid={target} "
            f"--vus {vus} "
            f"--duration {duration}"
        )

    testrun = TestRun(
        {
            "apiVersion": "k6.io/v1alpha1",
            "kind": "TestRun",
            "metadata": {"name": run_name, "namespace": K6_NAMESPACE},
            "spec": {
                "parallelism": parallelism,
                "arguments": arguments,
                "script": {"configMap": {"name": script_cm_name, "file": script_file}},
                "runner": {"env": env_vars},
                # The initializer pod evaluates the script for VU/options
                # introspection BEFORE any runner starts, so it needs the same
                # env (otherwise module-level `new AWSConfig({...})` blows up).
                "initializer": {"env": env_vars},
            },
        }
    )
    log.info(
        f"Submitting TestRun '{run_name}' "
        f"(vus={vus}, duration={duration}, parallelism={parallelism}"
        f"{', scenario=' + scenario if scenario else ''})..."
    )
    testrun.create()

    try:
        # SCENARIO presets like stress_cloud can total ~44 minutes; constant-VU
        # runs are bounded by the CLI --duration.
        run_timeout = 3600 if scenario else 1800
        _wait_for_condition(
            f"TestRun '{run_name}' to reach finished/error",
            lambda: _testrun_phase(run_name) in ("finished", "error", "stopped"),
            timeout=run_timeout,
            interval=5,
        )
        final_phase = _testrun_phase(run_name)
        logs = _stream_k6_logs(run_name)
        if final_phase == "error":
            raise RuntimeError(f"TestRun '{run_name}' ended in error phase")
        # The TestRun phase is 'finished' regardless of threshold pass/fail.
        # mixed.js emits a MINIROOK_VERDICT block via handleSummary() that
        # tells us which thresholds (if any) failed.
        verdict = _parse_k6_verdict(logs)
        if verdict is not None and not verdict.get("ok", True):
            failed = verdict.get("failed", [])
            log.error(
                f"[bold red]TestRun '{run_name}' finished but {len(failed)} threshold(s) failed:[/bold red]",
                extra={"markup": True},
            )
            for f in failed:
                log.error(f"  ✗ {f.get('metric', '?')}: {f.get('threshold', '?')}")
            raise RuntimeError(
                f"TestRun '{run_name}' failed {len(failed)} threshold(s): "
                + ", ".join(f.get("metric", "?") for f in failed)
            )
        log.info(
            f"[bold green]TestRun '{run_name}' {final_phase}.[/bold green]",
            extra={"markup": True},
        )
        log.info(
            "View results in Grafana: `python minirook.py forward-monitoring` -> "
            f"http://localhost:{GRAFANA_LOCAL_PORT} (filter by testid={target})"
        )
    finally:
        if not keep:
            log.info(f"Cleaning up TestRun '{run_name}' (pass --keep to retain)...")
            try:
                testrun.delete()
            except Exception as e:
                log.warning(f"Failed to delete TestRun: {e}")

# ==========================================
# Keystone/Barbican Configuration
# ==========================================
def _ks_token(
    keystone_url: str,
    username: str,
    password: str,
    project_name: str,
    domain: str = "Default",
) -> str:
    resp = httpx.post(
        f"{keystone_url}/v3/auth/tokens",
        json={
            "auth": {
                "identity": {
                    "methods": ["password"],
                    "password": {
                        "user": {
                            "name": username,
                            "password": password,
                            "domain": {"name": domain},
                        }
                    },
                },
                "scope": {
                    "project": {
                        "name": project_name,
                        "domain": {"name": domain},
                    }
                },
            }
        },
    )
    resp.raise_for_status()
    return resp.headers["X-Subject-Token"]


def setup_keystone(keystone_url: str) -> dict[str, str]:
    log.info("Setting up Keystone users, roles, and EC2 credentials...")

    admin_token = None
    for attempt in range(5):
        try:
            admin_token = _ks_token(keystone_url, "admin", "password", "admin")
            break
        except Exception as e:
            log.warning(f"Keystone warmup attempt {attempt + 1}/5 failed: {e}")
            time.sleep(5)
    if admin_token is None:
        raise RuntimeError("Keystone did not become reachable after 5 attempts.")

    headers = {"X-Auth-Token": admin_token}

    def _get_or_create_project(name: str) -> str:
        r = httpx.get(f"{keystone_url}/v3/projects", headers=headers, params={"name": name, "domain_id": "default"})
        r.raise_for_status()
        projects = r.json()["projects"]
        if projects:
            return str(projects[0]["id"])
        r = httpx.post(
            f"{keystone_url}/v3/projects", headers=headers, json={"project": {"name": name, "domain_id": "default"}}
        )
        r.raise_for_status()
        return str(r.json()["project"]["id"])

    def _get_role_id(name: str) -> str:
        r = httpx.get(f"{keystone_url}/v3/roles", headers=headers, params={"name": name})
        r.raise_for_status()
        return str(r.json()["roles"][0]["id"])

    def _get_or_create_user(name: str, password: str) -> str:
        r = httpx.get(f"{keystone_url}/v3/users", headers=headers, params={"name": name})
        r.raise_for_status()
        users = r.json()["users"]
        if users:
            user_id = users[0]["id"]
            # Always reset password so re-runs work even if a prior run left stale state
            httpx.patch(
                f"{keystone_url}/v3/users/{user_id}", headers=headers, json={"user": {"password": password}}
            ).raise_for_status()
            return str(user_id)
        r = httpx.post(
            f"{keystone_url}/v3/users",
            headers=headers,
            json={"user": {"name": name, "password": password, "domain_id": "default"}},
        )
        r.raise_for_status()
        return str(r.json()["user"]["id"])

    def _assign_role(project_id: str, user_id: str, role_id: str) -> None:
        r = httpx.put(
            f"{keystone_url}/v3/projects/{project_id}/users/{user_id}/roles/{role_id}",
            headers=headers,
        )
        # 204 = assigned, 409 = already exists — both are fine
        if r.status_code not in (204, 409):
            r.raise_for_status()

    service_project_id = _get_or_create_project("service")
    admin_project_id = _get_or_create_project("admin")
    admin_role_id = _get_role_id("admin")
    member_role_id = _get_role_id("member")

    rgw_service_id = _get_or_create_user("rgw-service", "rgw-service-pass")
    rgwcrypt_id = _get_or_create_user("rgwcrypt", "rgwcrypt-pass")
    test_user_id = _get_or_create_user("test-user", "test-pass")

    _assign_role(service_project_id, rgw_service_id, admin_role_id)
    _assign_role(service_project_id, rgwcrypt_id, member_role_id)
    _assign_role(admin_project_id, test_user_id, member_role_id)

    # Create fresh EC2 credentials for test-user via the OS-EC2 extension
    # Use admin token — newer Keystone policies restrict OS-EC2 self-service
    # Delete any stale credentials from previous runs first
    r = httpx.get(f"{keystone_url}/v3/users/{test_user_id}/credentials/OS-EC2", headers=headers)
    r.raise_for_status()
    for old_cred in r.json()["credentials"]:
        httpx.delete(
            f"{keystone_url}/v3/users/{test_user_id}/credentials/OS-EC2/{old_cred['access']}", headers=headers
        ).raise_for_status()

    r = httpx.post(
        f"{keystone_url}/v3/users/{test_user_id}/credentials/OS-EC2",
        headers=headers,
        json={"tenant_id": admin_project_id},
    )
    r.raise_for_status()
    cred = r.json()["credential"]

    log.info("[bold green]Keystone setup complete.[/bold green]", extra={"markup": True})
    log.info(
        f"EC2 credentials for manual testing:\n"
        f"  AWS_ACCESS_KEY_ID={cred['access']}\n"
        f"  AWS_SECRET_ACCESS_KEY={cred['secret']}\n"
        f"  S3 endpoint: http://localhost:{RGW_LOCAL_PORT}"
    )
    return {
        "rgwcrypt_user_id": rgwcrypt_id,
        "ec2_access": cred["access"],
        "ec2_secret": cred["secret"],
    }


def setup_barbican(keystone_url: str, rgwcrypt_user_id: str, barbican_url: str) -> str:
    log.info("Setting up Barbican SSE-KMS key...")
    token = _ks_token(keystone_url, "admin", "password", "admin")
    headers = {"X-Auth-Token": token}

    r = httpx.get(f"{barbican_url}/v1/secrets", headers=headers, params={"name": "rgw-sse-kms-key"})
    r.raise_for_status()
    secrets = r.json().get("secrets", [])

    # Delete existing secrets that have no payload (broken from prior runs)
    for s in secrets:
        if "content_types" not in s:
            ref = s["secret_ref"].rsplit("/", 1)[-1]
            httpx.delete(f"{barbican_url}/v1/secrets/{ref}", headers=headers).raise_for_status()
            secrets = []
            break

    if secrets and "content_types" in secrets[0]:
        secret_ref = secrets[0]["secret_ref"]
    else:
        key_material = base64.b64encode(os.urandom(32)).decode()
        r = httpx.post(
            f"{barbican_url}/v1/secrets",
            headers=headers,
            json={
                "name": "rgw-sse-kms-key",
                "algorithm": "aes",
                "bit_length": 256,
                "mode": "cbc",
                "secret_type": "symmetric",
                "payload": key_material,
                "payload_content_type": "application/octet-stream",
                "payload_content_encoding": "base64",
            },
        )
        r.raise_for_status()
        secret_ref = r.json()["secret_ref"]

    key_uuid = secret_ref.rsplit("/", 1)[-1]

    httpx.put(
        f"{barbican_url}/v1/secrets/{key_uuid}/acl",
        headers=headers,
        json={"read": {"users": [rgwcrypt_user_id], "project-access": False}},
    ).raise_for_status()

    log.info(f"[bold green]Barbican key ready: {key_uuid}[/bold green]", extra={"markup": True})
    return str(key_uuid)


def reconfigure_object_store_for_keystone(keystone_url: str) -> None:
    log.info("Reconfiguring CephObjectStore for Keystone + Barbican...")

    config_ini = (
        "[client]\n"
        "rgw crypt require ssl = false\n"
        "rgw_crypt_s3_kms_backend = barbican\n"
        f"rgw_barbican_url = {BARBICAN_CLUSTER_URL}\n"
        "rgw_keystone_barbican_user = rgwcrypt\n"
        "rgw_keystone_barbican_password = rgwcrypt-pass\n"
        "rgw_keystone_barbican_project = service\n"
        "rgw_keystone_barbican_domain = Default\n"
        "\n"
        "[global]\n"
        "debug rgw = 20\n"
    )
    _apply(
        ConfigMap(
            {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": "rook-config-override", "namespace": "rook-ceph"},
                "data": {"config": config_ini},
            }
        )
    )

    def _b64(s: str) -> str:
        return base64.b64encode(s.encode()).decode()

    _apply(
        Secret(
            {
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {"name": "rgw-keystone-creds", "namespace": "rook-ceph"},
                "type": "Opaque",
                "data": {
                    "OS_AUTH_TYPE": _b64("password"),
                    "OS_IDENTITY_API_VERSION": _b64("3"),
                    "OS_AUTH_URL": _b64(keystone_url),
                    "OS_USERNAME": _b64("rgw-service"),
                    "OS_PASSWORD": _b64("rgw-service-pass"),
                    "OS_PROJECT_NAME": _b64("service"),
                    "OS_USER_DOMAIN_NAME": _b64("Default"),
                    "OS_PROJECT_DOMAIN_NAME": _b64("Default"),
                },
            }
        )
    )

    _apply(
        CephObjectStore(
            {
                "apiVersion": "ceph.rook.io/v1",
                "kind": "CephObjectStore",
                "metadata": {"name": "my-store", "namespace": "rook-ceph"},
                "spec": {
                    "metadataPool": {"failureDomain": "osd", "replicated": {"size": 1}},
                    "dataPool": {"failureDomain": "osd", "replicated": {"size": 1}},
                    "gateway": {
                        "port": 80,
                        "instances": 1,
                        "opsLogSidecar": {"resources": {"requests": {}, "limits": {}}},
                    },
                    "auth": {
                        "keystone": {
                            "url": keystone_url,
                            "serviceUserSecretName": "rgw-keystone-creds",
                            "acceptedRoles": ["admin", "member"],
                            "implicitTenants": "swift",
                            "revocationInterval": 1200,
                            "tokenCacheSize": 1000,
                        }
                    },
                    "protocols": {
                        "s3": {"authUseKeystone": True},
                    },
                },
            }
        )
    )

    # Wait for the Rook operator to finish reconciling the CephObjectStore.
    # Rook writes the rgw_keystone_* settings to the central mon config DB
    # asynchronously (~10s of writes per reconcile). If we restart RGW pods
    # before that's done, the new pods read mon config without keystone
    # settings and never enable KeystoneEngine in their auth chain.
    log.info("Waiting for Rook to reconcile CephObjectStore (mon config writes)...")

    def _store_reconciled() -> bool:
        stores = list(kr8s.get("cephobjectstores", "my-store", namespace="rook-ceph"))
        if not stores:
            return False
        raw = stores[0].raw
        gen = raw.get("metadata", {}).get("generation")
        observed = raw.get("status", {}).get("observedGeneration")
        phase = raw.get("status", {}).get("phase")
        return gen is not None and gen == observed and phase == "Ready"

    _wait_for_condition("CephObjectStore reconciled", _store_reconciled, timeout=300, interval=5)

    # Now restart RGW pods so they read the up-to-date mon config.
    log.info("Restarting RGW pods to apply new configuration...")
    rgw_pods = list(kr8s.get("pods", namespace="rook-ceph", label_selector={"app": "rook-ceph-rgw"}))
    for pod in rgw_pods:
        pod.delete()
    time.sleep(5)

    def _rgw_pods_ready() -> bool:
        pods = list(kr8s.get("pods", namespace="rook-ceph", label_selector={"app": "rook-ceph-rgw"}))
        if not pods:
            return False
        return all(
            any(
                c.get("type") == "Ready" and c.get("status") == "True"
                for c in p.raw.get("status", {}).get("conditions", [])
            )
            for p in pods
        )

    _wait_for_condition("RGW pods to be Ready", _rgw_pods_ready, timeout=300, interval=5)


# ==========================================
# OpenStack Smoke Tests
# ==========================================
def run_openstack_smoke_tests(ec2_access: str, ec2_secret: str, key_uuid: str) -> None:
    log.info("Running OpenStack (Keystone + SSE-KMS) smoke tests...")
    log.info(f"  EC2 access key: {ec2_access}")
    log.info(f"  KMS key UUID:   {key_uuid}")

    s3 = boto3.client(
        "s3",
        endpoint_url=f"http://localhost:{RGW_LOCAL_PORT}",
        aws_access_key_id=ec2_access,
        aws_secret_access_key=ec2_secret,
        region_name="us-east-1",
    )

    # Keystone auth test
    bucket = "keystone-smoke-test"
    key = "hello-ks.txt"
    body = b"Hello from Keystone-authenticated smoke test!"

    # After RGW restart, Keystone EC2 auth may not be ready immediately.
    # Retry the first S3 call until the RGW-Keystone integration is live.
    log.info("Waiting for RGW Keystone EC2 auth to be ready...")
    for attempt in range(30):
        try:
            s3.create_bucket(Bucket=bucket)
            break
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code in ("InvalidAccessKeyId", "SignatureDoesNotMatch") and attempt < 29:
                log.info(f"  attempt {attempt + 1}/30: {code}, retrying in 5s...")
                time.sleep(5)
            elif code == "BucketAlreadyOwnedByYou":
                break
            else:
                raise
    else:
        raise RuntimeError("RGW Keystone EC2 auth did not become ready in time")
    log.info(f"[green]✓ Keystone: Created bucket '{bucket}'[/]", extra={"markup": True})

    s3.put_object(Bucket=bucket, Key=key, Body=body)
    log.info(f"[green]✓ Keystone: Uploaded object '{key}'[/]", extra={"markup": True})

    got = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    assert got == body, f"Keystone body mismatch: {got!r}"
    log.info(f"[green]✓ Keystone: Downloaded and verified '{key}'[/]", extra={"markup": True})

    s3.delete_object(Bucket=bucket, Key=key)

    # SSE-KMS test
    kms_key = "hello-kms.txt"
    kms_body = b"Hello from SSE-KMS encrypted smoke test!"
    s3.put_object(
        Bucket=bucket,
        Key=kms_key,
        Body=kms_body,
        ServerSideEncryption="aws:kms",
        SSEKMSKeyId=key_uuid,
    )
    log.info(f"[green]✓ SSE-KMS: Uploaded encrypted object '{kms_key}'[/]", extra={"markup": True})

    got_kms = s3.get_object(Bucket=bucket, Key=kms_key)["Body"].read()
    assert got_kms == kms_body, f"SSE-KMS body mismatch: {got_kms!r}"
    log.info(f"[green]✓ SSE-KMS: Downloaded and verified '{kms_key}'[/]", extra={"markup": True})

    s3.delete_object(Bucket=bucket, Key=kms_key)
    s3.delete_bucket(Bucket=bucket)
    log.info(f"[green]✓ Cleaned up bucket '{bucket}'[/]", extra={"markup": True})

    log.info("[bold green]All OpenStack smoke tests passed![/bold green]", extra={"markup": True})


# ==========================================
# CLI
# ==========================================
@click.group(help="Automate Minikube & Rook + OpenStack test environment setup.")
def cli() -> None:
    pass


@cli.command("setup", help="Run the full setup pipeline.")
@click.argument("image_name", required=False, default=None)
@click.option(
    "--image-transfer",
    type=click.Choice(["auto", "load", "remote"], case_sensitive=False),
    default="auto",
    show_default=True,
    help=(
        "How to make the image available to the cluster. "
        "'auto': load if found in local Podman, otherwise pull remotely. "
        "'load': always export from Podman and load into Minikube. "
        "'remote': always let pods pull the image from a registry."
    ),
)
def setup_cmd(image_name: str | None, image_transfer: str) -> None:
    """
    IMAGE_NAME: Optional container image to use for Ceph daemons.
    If provided, the image will be loaded and configured.
    If omitted, the stock image is used.
    """
    main(image_name, image_transfer)


def main(image_name: str | None, image_transfer: str) -> None:
    log.info("[bold magenta]Starting Rook-Ceph Test Environment Setup[/]", extra={"markup": True})

    # 1. Minikube
    if is_minikube_running():
        log.info("Minikube already running, skipping.")
    else:
        start_minikube()

    kr8s.api()

    # 2. Rook + Ceph
    if is_rook_deployed() and is_ceph_healthy():
        log.info("Rook deployed and Ceph healthy, skipping.")
    else:
        deploy_rook()
        _wait_for_condition("Ceph HEALTH_OK", is_ceph_healthy, timeout=300, interval=10)

    # 3. Custom image
    if image_name is not None:
        if image_transfer == "load" or (image_transfer == "auto" and is_local_image(image_name)):
            log.info(f"Image [bold]{image_name}[/] will be loaded into Minikube from Podman.", extra={"markup": True})
            load_image_to_minikube(image_name)
        else:
            log.info(f"Image [bold]{image_name}[/] will be pulled from a registry by the pods.", extra={"markup": True})

        upgrade_rook_operator(image_name)
        _wait_for_condition("Ceph HEALTH_OK", is_ceph_healthy, timeout=300, interval=10)

    # 4. Object store
    if is_object_store_ready():
        log.info("CephObjectStore already Ready, skipping.")
    else:
        deploy_object_store()

    # 5. Basic S3 smoke test (skip if Keystone already configured)
    if is_object_store_keystone_configured():
        log.info("Keystone already configured, skipping basic S3 smoke test.")
    else:
        with port_forward("rook-ceph-rgw-my-store", "rook-ceph", RGW_LOCAL_PORT, 80):
            run_smoke_tests()

    # 6. OpenStack namespace
    if is_namespace_exists("openstack"):
        log.info("Namespace 'openstack' exists, skipping.")
    else:
        setup_openstack_namespace()

    # 7. OpenStack infra (addons + MariaDB)
    if is_mariadb_deployed():
        log.info("MariaDB already deployed, skipping infra step.")
    else:
        deploy_openstack_infra()

    # 8. Keystone (native)
    if is_keystone_native_deployed():
        log.info("Keystone already deployed, skipping.")
    else:
        deploy_keystone_native()

    # 9. Barbican (native)
    if is_barbican_native_deployed():
        log.info("Barbican already deployed, skipping.")
    else:
        deploy_barbican_native()

    # 10. Configure Keystone + Barbican (always runs, idempotent, needed for creds)
    with (
        port_forward("keystone-api", "openstack", KEYSTONE_LOCAL_PORT, 5000),
        port_forward("barbican-api", "openstack", BARBICAN_LOCAL_PORT, 9311),
    ):
        ks_result = setup_keystone(f"http://localhost:{KEYSTONE_LOCAL_PORT}")
        key_uuid = setup_barbican(
            keystone_url=f"http://localhost:{KEYSTONE_LOCAL_PORT}",
            rgwcrypt_user_id=ks_result["rgwcrypt_user_id"],
            barbican_url=f"http://localhost:{BARBICAN_LOCAL_PORT}",
        )

    # 11. Reconfigure ObjectStore for Keystone (always re-apply, idempotent)
    reconfigure_object_store_for_keystone(KEYSTONE_CLUSTER_URL)

    # 12. OpenStack smoke tests
    with port_forward("rook-ceph-rgw-my-store", "rook-ceph", RGW_LOCAL_PORT, 80):
        _wait_for_condition(
            "RGW API to be reachable",
            lambda: httpx.get(f"http://localhost:{RGW_LOCAL_PORT}").status_code in (200, 403),
            timeout=60,
            interval=3,
        )
        run_openstack_smoke_tests(ks_result["ec2_access"], ks_result["ec2_secret"], key_uuid)

    log.info("[bold green]All done![/bold green]", extra={"markup": True})


def _get_ec2_credentials(keystone_url: str, user_name: str = "test-user") -> tuple[str, str]:
    """Fetch EC2 access/secret for a Keystone user. Raises if user or credential is missing."""
    token = _ks_token(keystone_url, "admin", "password", "admin")
    headers = {"X-Auth-Token": token}

    r = httpx.get(f"{keystone_url}/v3/users", headers=headers, params={"name": user_name})
    r.raise_for_status()
    users = r.json()["users"]
    if not users:
        raise RuntimeError(f"Keystone user '{user_name}' not found")
    user_id = users[0]["id"]

    r = httpx.get(f"{keystone_url}/v3/users/{user_id}/credentials/OS-EC2", headers=headers)
    r.raise_for_status()
    creds = r.json()["credentials"]
    if not creds:
        raise RuntimeError(f"No EC2 credentials for Keystone user '{user_name}'")
    return creds[0]["access"], creds[0]["secret"]


def _get_barbican_kms_key(keystone_url: str, barbican_url: str, name: str = "rgw-sse-kms-key") -> str | None:
    """Look up a Barbican secret UUID by name. Returns None if not present."""
    token = _ks_token(keystone_url, "admin", "password", "admin")
    headers = {"X-Auth-Token": token}
    r = httpx.get(f"{barbican_url}/v1/secrets", headers=headers, params={"name": name})
    r.raise_for_status()
    secrets = r.json().get("secrets", [])
    if not secrets:
        return None
    return str(secrets[0]["secret_ref"].rsplit("/", 1)[-1])


def _create_barbican_secret(
    barbican_url: str, token: str, name: str, rgwcrypt_user_id: str
) -> str:
    """Create a new Barbican AES-256 secret and grant rgwcrypt read access.
    Without the ACL grant, RGW (which authenticates to Barbican as the
    rgwcrypt user when serving SSE-KMS) gets 403 on the key fetch, which
    surfaces to S3 callers as PUT/GET 403 AccessDenied."""
    payload_b64 = base64.b64encode(os.urandom(32)).decode("ascii")
    body = {
        "name": name,
        "payload": payload_b64,
        "payload_content_type": "application/octet-stream",
        "payload_content_encoding": "base64",
        "algorithm": "aes",
        "bit_length": 256,
        "mode": "cbc",
    }
    r = httpx.post(
        f"{barbican_url}/v1/secrets",
        headers={"X-Auth-Token": token, "Content-Type": "application/json"},
        json=body,
    )
    r.raise_for_status()
    secret_ref = r.json()["secret_ref"]
    uuid = str(secret_ref.rsplit("/", 1)[-1])

    httpx.put(
        f"{barbican_url}/v1/secrets/{uuid}/acl",
        headers={"X-Auth-Token": token, "Content-Type": "application/json"},
        json={"read": {"users": [rgwcrypt_user_id], "project-access": False}},
    ).raise_for_status()
    return uuid


def _fetch_forward_info() -> dict[str, str | None]:
    """Fetch EC2 credentials and Barbican KMS key UUID for manual testing."""
    info: dict[str, str | None] = {"access": None, "secret": None, "kms_key": None}
    keystone_url = f"http://localhost:{KEYSTONE_LOCAL_PORT}"
    barbican_url = f"http://localhost:{BARBICAN_LOCAL_PORT}"
    try:
        info["access"], info["secret"] = _get_ec2_credentials(keystone_url)
    except Exception as e:
        log.warning(f"Could not fetch EC2 credentials: {e}")
    try:
        info["kms_key"] = _get_barbican_kms_key(keystone_url, barbican_url)
    except Exception as e:
        log.warning(f"Could not fetch Barbican KMS key: {e}")
    return info


ENV_FILE = "minirook-env.sh"


def _write_env_file(access: str, secret: str, kms_key: str | None) -> None:
    content = (
        f"# Generated by minirook.py forward — source this file:\n"
        f"#   source {ENV_FILE}\n"
        f"export ACCESS_KEY='{access}'\n"
        f"export SECRET_KEY='{secret}'\n"
    )
    if kms_key:
        content += f"export KMS_KEY='{kms_key}'\n"
    content += (
        f"\n"
        f"s3() {{\n"
        f"  s3cmd \\\n"
        f'    --access_key="$ACCESS_KEY" \\\n'
        f'    --secret_key="$SECRET_KEY" \\\n'
        f"    --host=localhost:{RGW_LOCAL_PORT} \\\n"
        f"    --host-bucket=localhost:{RGW_LOCAL_PORT} \\\n"
        f"    --no-ssl \\\n"
        f'    "$@"\n'
        f"}}\n"
    )
    with open(ENV_FILE, "w") as f:
        f.write(content)


@cli.command("forward", help="Port-forward all endpoints for manual testing. Ctrl+C to stop.")
def forward_cmd() -> None:
    kr8s.api()
    forwards: list[subprocess.Popen[bytes]] = []
    endpoints = [
        ("rook-ceph-rgw-my-store", "rook-ceph", RGW_LOCAL_PORT, 80),
        ("keystone-api", "openstack", KEYSTONE_LOCAL_PORT, 5000),
        ("barbican-api", "openstack", BARBICAN_LOCAL_PORT, 9311),
    ]
    try:
        for svc, ns, local_port, remote_port in endpoints:
            log.info(f"Port-forwarding svc/{svc} in {ns} -> localhost:{local_port}...")
            pf = subprocess.Popen(
                ["kubectl", "port-forward", f"svc/{svc}", f"{local_port}:{remote_port}", "-n", ns],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            forwards.append(pf)

        # Wait for port-forwards to be ready before fetching creds
        import socket

        for port in (KEYSTONE_LOCAL_PORT, BARBICAN_LOCAL_PORT):
            for _ in range(30):
                time.sleep(1)
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=1):
                        break
                except OSError:
                    pass

        info = _fetch_forward_info()

        log.info("")
        log.info("[bold green]All port-forwards active:[/]", extra={"markup": True})
        log.info(f"  RGW S3:    http://localhost:{RGW_LOCAL_PORT}")
        log.info(f"  Keystone:  http://localhost:{KEYSTONE_LOCAL_PORT}")
        log.info(f"  Barbican:  http://localhost:{BARBICAN_LOCAL_PORT}")

        access, secret, kms_key = info["access"], info["secret"], info["kms_key"]
        if access and secret:
            _write_env_file(access, secret, kms_key)
            log.info("")
            log.info("[bold green]EC2 credentials:[/]", extra={"markup": True})
            log.info(f"  ACCESS_KEY={info['access']}")
            log.info(f"  SECRET_KEY={info['secret']}")
            if info["kms_key"]:
                log.info(f"  KMS_KEY={info['kms_key']}")
            log.info("")
            log.info(f"Shell helper written to [bold]{ENV_FILE}[/]. Usage:", extra={"markup": True})
            log.info(f"  source {ENV_FILE}")
            log.info("  s3 ls")
            log.info("  s3 mb s3://my-bucket")
            log.info("  s3 put ./file.txt s3://my-bucket/")
            if info["kms_key"]:
                log.info(
                    "  s3 put --server-side-encryption --server-side-encryption-kms-id=$KMS_KEY ./file.txt s3://my-bucket/encrypted.txt"
                )
        else:
            log.warning("No EC2 credentials found. Run 'setup' first.")

        log.info("")
        log.info("Press Ctrl+C to stop.")

        # Block until Ctrl+C or a port-forward dies
        while True:
            for pf in forwards:
                if pf.poll() is not None:
                    log.warning(f"A port-forward exited (pid {pf.pid}, code {pf.returncode}), shutting down.")
                    raise KeyboardInterrupt
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Stopping port-forwards...")
    finally:
        for pf in forwards:
            pf.terminate()
        for pf in forwards:
            pf.wait()
        log.info("All port-forwards stopped.")


@cli.command("monitoring", help="Deploy Prometheus + Grafana + mtail for Ceph/RGW observability.")
def monitoring_cmd() -> None:
    kr8s.api()
    # Always reconcile: `helm upgrade --install` is idempotent, and reconciling
    # is the only way to apply new flag changes (e.g. enableRemoteWriteReceiver,
    # native-histograms) to an existing deployment without manual intervention.
    deploy_monitoring()
    # Always reconcile the full mtail stack — every _apply is idempotent,
    # the readiness wait is fast when already Ready, and unconditionally
    # re-applying self-heals partial deployments (e.g. a prior run that
    # died before creating the PodMonitor).
    deploy_mtail()
    log.info(
        f"Run `python minirook.py forward-monitoring` to expose Grafana on "
        f"localhost:{GRAFANA_LOCAL_PORT} (admin / admin)."
    )


@cli.command("forward-monitoring", help="Port-forward Grafana, Prometheus, and (if present) mtail. Ctrl+C to stop.")
def forward_monitoring_cmd() -> None:
    kr8s.api()
    if not is_monitoring_deployed():
        log.warning("Monitoring stack is not deployed. Run `monitoring` first.")
        return
    forwards: list[subprocess.Popen[bytes]] = []
    endpoints = [
        ("kube-prometheus-stack-grafana", MONITORING_NAMESPACE, GRAFANA_LOCAL_PORT, 80),
        ("kube-prometheus-stack-prometheus", MONITORING_NAMESPACE, PROMETHEUS_LOCAL_PORT, 9090),
    ]
    mtail_active = is_mtail_deployed()
    if mtail_active:
        endpoints.append(("mtail", MONITORING_NAMESPACE, MTAIL_METRICS_PORT, MTAIL_METRICS_PORT))
    try:
        for svc, ns, local_port, remote_port in endpoints:
            log.info(f"Port-forwarding svc/{svc} in {ns} -> localhost:{local_port}...")
            pf = subprocess.Popen(
                ["kubectl", "port-forward", f"svc/{svc}", f"{local_port}:{remote_port}", "-n", ns],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            forwards.append(pf)

        log.info("")
        log.info("[bold green]Monitoring port-forwards active:[/]", extra={"markup": True})
        log.info(f"  Grafana:    http://localhost:{GRAFANA_LOCAL_PORT}  (admin / admin)")
        log.info(f"  Prometheus: http://localhost:{PROMETHEUS_LOCAL_PORT}")
        if mtail_active:
            log.info(f"  mtail:      http://localhost:{MTAIL_METRICS_PORT}/metrics")
        log.info("")
        log.info("Press Ctrl+C to stop.")

        while True:
            for pf in forwards:
                if pf.poll() is not None:
                    log.warning(f"A port-forward exited (pid {pf.pid}, code {pf.returncode}), shutting down.")
                    raise KeyboardInterrupt
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Stopping port-forwards...")
    finally:
        for pf in forwards:
            pf.terminate()
        for pf in forwards:
            pf.wait()
        log.info("All port-forwards stopped.")


@cli.group("k6", help="Drive k6 load tests via the k6-operator.")
def k6_cmd() -> None:
    pass


@k6_cmd.command("install", help="Install k6-operator and import the k6 Grafana dashboard.")
def k6_install_cmd() -> None:
    kr8s.api()
    if not is_monitoring_deployed():
        log.warning("Monitoring stack is not deployed. Run `monitoring` first to enable Prometheus remote-write.")
        return
    if is_k6_operator_deployed():
        log.info("k6-operator already deployed, refreshing dashboard only.")
        try:
            _apply(_k6_dashboard_configmap(_fetch_k6_dashboard()))
        except Exception as e:
            log.warning(f"Could not import k6 Grafana dashboard: {e}")
        return
    install_k6_operator()


@k6_cmd.command("run", help="Run a k6 scenario against the live stack.")
@click.argument("target", type=click.Choice(list(_K6_RUN_TARGETS), case_sensitive=False))
@click.option("--vus", type=int, default=5, show_default=True, help="Virtual users (ignored when --scenario is set).")
@click.option("--duration", type=str, default="30s", show_default=True, help="Test duration (ignored when --scenario is set).")
@click.option("--parallelism", type=int, default=1, show_default=True, help="Number of k6-operator runner pods.")
@click.option("--keep", is_flag=True, help="Leave the TestRun resource around after completion.")
@click.option("--weights", type=str, default=None, help="[mixed] op weights, e.g. 'get=60,put=30,delete=10' or 'put=100'.")
@click.option(
    "--preseed",
    type=str,
    default=None,
    help="[mixed] preseed mode: 'none', '<duration>' (e.g. '30s'), or 'existing' (LIST the bucket).",
)
@click.option("--preseed-list-max", type=int, default=None, help="[mixed] cap for PRESEED=existing LIST pagination.")
@click.option("--object-size", type=int, default=None, help="[mixed] PUT body size in bytes.")
@click.option(
    "--scenario",
    type=click.Choice(list(_MIXED_SCENARIOS), case_sensitive=False),
    default=None,
    help="[mixed] use a load-shape preset instead of --vus/--duration. 'constant' requires --rate.",
)
@click.option(
    "--rate",
    type=int,
    default=None,
    help="[mixed] constant-arrival-rate iter/s. Implies --scenario constant; --duration sets how long.",
)
@click.option(
    "--buckets",
    type=int,
    default=1,
    show_default=True,
    help="[mixed] Number of buckets to spread load across (named k6-bench, or k6-bench-0..N-1).",
)
@click.option(
    "--sse",
    type=click.Choice(["none", "request", "bucket"], case_sensitive=False),
    default=None,
    help=(
        "[mixed] SSE-KMS mode. 'none': no encryption. 'request': per-PUT SSE-KMS "
        "headers with one Barbican secret per bucket. 'bucket': PutBucketEncryption "
        "default-encryption per bucket. Default: 'bucket' if keystone+barbican are "
        "deployed, else 'none'."
    ),
)
@click.option(
    "--skip-setup",
    is_flag=True,
    help=(
        "[mixed] Skip bucket/Barbican setup; assume `k6 prepare-buckets` already ran. "
        "Reads bucket names from --buckets; relies on bucket-level encryption being "
        "already configured server-side."
    ),
)
def k6_run_cmd(
    target: str,
    vus: int,
    duration: str,
    parallelism: int,
    keep: bool,
    weights: str | None,
    preseed: str | None,
    preseed_list_max: int | None,
    object_size: int | None,
    scenario: str | None,
    rate: int | None,
    buckets: int,
    sse: str | None,
    skip_setup: bool,
) -> None:
    kr8s.api()
    if not is_k6_operator_deployed():
        log.warning("k6-operator is not deployed. Run `k6 install` first.")
        return
    run_k6_scenario(
        target,
        vus,
        duration,
        keep,
        parallelism=parallelism,
        weights=weights,
        preseed=preseed,
        preseed_list_max=preseed_list_max,
        object_size=object_size,
        scenario=scenario,
        rate=rate,
        buckets=buckets,
        sse_mode=sse,
        skip_setup=skip_setup,
    )


@k6_cmd.command(
    "prepare-buckets",
    help=(
        "Pre-create k6-bench bucket(s) and (optionally) per-bucket default "
        "SSE-KMS encryption. Idempotent: re-running reuses existing buckets "
        "and the KMSMasterKeyID currently set on them."
    ),
)
@click.option(
    "--buckets",
    type=int,
    default=1,
    show_default=True,
    help="Number of buckets to create (k6-bench, or k6-bench-0..N-1).",
)
@click.option(
    "--sse",
    type=click.Choice(["none", "bucket"], case_sensitive=False),
    default=None,
    help=(
        "SSE-KMS mode. 'bucket' = PutBucketEncryption default encryption with "
        "one Barbican secret per bucket. Default: 'bucket' if keystone+barbican "
        "are deployed, else 'none'."
    ),
)
def k6_prepare_buckets_cmd(buckets: int, sse: str | None) -> None:
    kr8s.api()
    if sse is None:
        sse = (
            "bucket"
            if is_keystone_native_deployed() and is_barbican_native_deployed()
            else "none"
        )
    _setup_mixed_workload(buckets, sse)


@k6_cmd.command(
    "teardown-buckets",
    help=(
        "Empty and delete k6-bench bucket(s). With --purge-secrets, also "
        "deletes the Barbican secrets referenced by each bucket's encryption "
        "config (does not chase orphaned secrets)."
    ),
)
@click.option(
    "--buckets",
    type=int,
    default=1,
    show_default=True,
    help="Number of buckets to remove (matches --buckets used at prepare time).",
)
@click.option(
    "--purge-secrets",
    is_flag=True,
    help="Also delete the per-bucket Barbican secrets discovered via get_bucket_encryption.",
)
def k6_teardown_buckets_cmd(buckets: int, purge_secrets: bool) -> None:
    kr8s.api()
    _teardown_mixed_workload(buckets, purge_secrets)


if __name__ == "__main__":
    cli()
