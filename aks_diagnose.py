#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
aks_diagnose v2 — AKS 처리 구조(Aggregator / Detector / Trainer) 진단 도구

pg_diagnose 철학(다계층 수집 → 휴리스틱 분석 → 자체 완결형 HTML 리포트) + 두 확장:
  · 계층 K  Kubernetes API        — 파드/재시작/OOMKilled/Pending/limits/HPA/Events
  · 계층 P  Prometheus (PromQL)    — CPU throttling, mem/limit, consumer lag, p99, GPU(DCGM)
  · 계층 T  Tracing (App Insights) — 컴포넌트별/홉별 latency·실패율 (OTel)           ← v2 (깊이)
  · 상관    메트릭 ↔ 트레이스       — 예: Detector p99↑ + throttling↑ → "CPU 한계가 지연 원인"
  · 운영    Blob 적재 + Baseline    — CronJob 무인 실행 시 리포트/이력을 Blob 에 영속화  ← v2 (운영)

안전: 읽기 전용. K8s get/list, PromQL query, App Insights KQL query, Blob 읽기/쓰기(이력·리포트)만.
부분 실패 허용: 각 계층/쿼리 독립 예외 처리.

실행:
  python aks_diagnose.py --demo --out report.html
  # 운영(클러스터 내 CronJob): k8s/cronjob.yaml 참고
"""
from __future__ import annotations

import argparse
import datetime as dt
import glob
import html
import json
import os
import re
import sys
from dataclasses import dataclass, field
from typing import Any, Optional

COMPONENTS: list[str] = []   # 동적으로 K8sCollector가 채움 (Deployment label 기반)
COMP_LABEL_KO: dict[str, str] = {"기타": "기타", "흐름": "흐름"}  # 동적으로 추가됨

# Kubernetes control-plane/static pod 이름 기반 매핑.
# static pod에는 일반 애플리케이션처럼 component 라벨이 없는 경우가 많으므로
# Pod 이름/컨테이너 이름으로 컴포넌트를 보강한다.
CONTROL_PLANE_PREFIXES: dict[str, str] = {
    "etcd-": "etcd",
    "kube-apiserver-": "kube-apiserver",
    "kube-controller-manager-": "kube-controller-manager",
    "kube-scheduler-": "kube-scheduler",
    "coredns-": "coredns",
    "kube-proxy-": "kube-proxy",
}

CONTROL_PLANE_LABELS: dict[str, str] = {
    "etcd": "Etcd",
    "kube-apiserver": "Kube-apiserver",
    "kube-controller-manager": "Kube-controller-manager",
    "kube-scheduler": "Kube-scheduler",
    "coredns": "CoreDNS",
    "kube-proxy": "Kube-proxy",
}


def _register_component(name: str) -> None:
    """Deployment에서 발견된 컴포넌트를 등록 (중복 방지)."""
    if name not in COMPONENTS and name not in ("기타", "흐름"):
        COMPONENTS.append(name)
        COMP_LABEL_KO[name] = CONTROL_PLANE_LABELS.get(name, name.capitalize())


# ──────────────────────────────────────────────────────────────────────────
@dataclass
class Config:
    namespace: str = "default"
    all_namespaces: bool = False       # -A: 전체 네임스페이스
    context: Optional[str] = None
    in_cluster: bool = False
    component_label: str = "component"
    k8s: bool = True
    # 계층 P
    prometheus_url: Optional[str] = None
    prometheus_aad: bool = False
    hours: int = 1
    step_min: int = 1
    lag_query: str = 'sum(kafka_consumergroup_lag{namespace="$NS"})'
    latency_query: str = ('histogram_quantile(0.99, sum(rate('
                          'http_request_duration_seconds_bucket{namespace="$NS"}[5m])) by (le))')
    gpu_query: str = 'avg(DCGM_FI_DEV_GPU_UTIL{namespace="$NS"})'
    # 계층 T (트레이싱)
    appinsights_id: Optional[str] = None           # App Insights 리소스 ID 또는 LA 워크스페이스 ID
    trace_hours: int = 1
    trace_table_mode: str = "classic"              # classic(requests/dependencies) | workspace(AppRequests/AppDependencies)
    # 운영 (Blob)
    blob_account_url: Optional[str] = None         # https://<acct>.blob.core.windows.net
    blob_container: str = "aks-diagnose"
    # Baseline
    history: bool = True
    history_dir: str = "./aks_diagnose_history"
    out: str = "aks_report.html"
    demo: bool = False


# ──────────────────────────────────────────────────────────────────────────
# 공통
# ──────────────────────────────────────────────────────────────────────────
SEV_CRIT, SEV_WARN, SEV_INFO, SEV_OK = "critical", "warning", "info", "ok"
SEV_WEIGHT = {SEV_CRIT: 25, SEV_WARN: 10, SEV_INFO: 2, SEV_OK: 0}
SEV_LABEL = {SEV_CRIT: "위험", SEV_WARN: "주의", SEV_INFO: "정보", SEV_OK: "양호"}


@dataclass
class Finding:
    severity: str
    component: str
    title: str
    detail: str
    recommendation: str
    steps: list[str] = field(default_factory=list)


@dataclass
class PodInfo:
    name: str
    component: str
    phase: str
    ready: bool
    restarts: int
    waiting_reason: str = ""
    term_reason: str = ""
    has_limits: bool = True
    node: str = ""
    namespace: str = ""


@dataclass
class EventInfo:
    reason: str
    obj: str
    message: str
    count: int


@dataclass
class HPAInfo:
    name: str
    component: str
    current: Optional[int]
    desired: Optional[int]
    minr: Optional[int]
    maxr: Optional[int]


@dataclass
class PVCInfo:
    name: str
    namespace: str
    status: str          # Bound / Pending / Lost
    capacity: str
    storage_class: str
    pod: str = ""


@dataclass
class PDBInfo:
    name: str
    component: str
    min_available: Optional[str]
    max_unavailable: Optional[str]
    current_healthy: Optional[int]
    desired_healthy: Optional[int]
    disruptions_allowed: Optional[int]


@dataclass
class NodeInfo:
    name: str
    ready: bool
    conditions: list[str] = field(default_factory=list)   # NotReady 등 비정상 조건
    cpu_alloc: str = ""
    mem_alloc: str = ""
    taints: list[str] = field(default_factory=list)


@dataclass
class ImageInfo:
    pod: str
    container: str
    image: str
    has_latest_tag: bool
    has_no_tag: bool


@dataclass
class K8sData:
    pods: list[PodInfo] = field(default_factory=list)
    events: list[EventInfo] = field(default_factory=list)
    hpas: list[HPAInfo] = field(default_factory=list)
    pvcs: list[PVCInfo] = field(default_factory=list)
    pdbs: list[PDBInfo] = field(default_factory=list)
    nodes: list[NodeInfo] = field(default_factory=list)
    images: list[ImageInfo] = field(default_factory=list)
    error: Optional[str] = None


@dataclass
class MetricSeries:
    name: str
    unit: str
    values: list[Optional[float]] = field(default_factory=list)
    error: Optional[str] = None

    @property
    def avg(self) -> Optional[float]:
        v = [x for x in self.values if x is not None]
        return round(sum(v) / len(v), 1) if v else None

    @property
    def mx(self) -> Optional[float]:
        v = [x for x in self.values if x is not None]
        return round(max(v), 1) if v else None


@dataclass
class PromData:
    enabled: bool = False
    error: Optional[str] = None
    per_pod: dict[str, dict[str, float]] = field(default_factory=dict)
    specialty: dict[str, Optional[float]] = field(default_factory=dict)
    series: dict[str, MetricSeries] = field(default_factory=dict)
    pvc_usage: dict[str, dict] = field(default_factory=dict)    # pvc_name → {namespace, used_pct, used_bytes, cap_bytes}
    node_usage: dict[str, dict] = field(default_factory=dict)   # node_name → {cpu_req_pct, mem_req_pct, cpu_req, cpu_alloc, mem_req, mem_alloc}
    pod_cpu_actual: dict[str, float] = field(default_factory=dict)  # pod_name → actual CPU cores (rate)


# 계층 T 구조
@dataclass
class CompLatency:
    role: str
    p95_ms: Optional[float]
    p99_ms: Optional[float]
    fail_pct: Optional[float]
    count: int


@dataclass
class HopStat:
    source: str
    target: str
    p95_ms: Optional[float]
    p99_ms: Optional[float]
    fail_pct: Optional[float]
    count: int


@dataclass
class TraceData:
    enabled: bool = False
    error: Optional[str] = None
    comp: dict[str, CompLatency] = field(default_factory=dict)
    hops: list[HopStat] = field(default_factory=list)


# ──────────────────────────────────────────────────────────────────────────
# 계층 K — Kubernetes API
# ──────────────────────────────────────────────────────────────────────────
class K8sCollector:
    def __init__(self, cfg: Config):
        self.cfg = cfg

    def _comp_of(self, labels: dict, pod_name: str = "") -> str:
        v = (labels or {}).get(self.cfg.component_label, "").strip()
        if v:
            _register_component(v)
            return v

        # static pod / control-plane 컴포넌트는 component 라벨이 없는 경우가 많다.
        # 이름 기반 fallback이 없으면 Etcd, kube-apiserver 등이 전부 "기타"로 묶여
        # Prometheus pod 라벨과 rollup 매칭이 실패한다.
        for prefix, comp in CONTROL_PLANE_PREFIXES.items():
            if pod_name.startswith(prefix):
                _register_component(comp)
                return comp
        return "기타"

    def _scan_deployments(self, apps_api, ns: str, all_ns: bool) -> None:
        """Deployment의 label을 스캔해서 컴포넌트를 동적 등록."""
        try:
            items = (apps_api.list_deployment_for_all_namespaces().items if all_ns
                     else apps_api.list_namespaced_deployment(ns).items)
            for dep in items:
                labels = (dep.spec.template.metadata.labels or {}) if dep.spec and dep.spec.template and dep.spec.template.metadata else {}
                v = labels.get(self.cfg.component_label, "").strip()
                if v:
                    _register_component(v)
        except Exception:  # noqa: BLE001
            pass

    def collect(self) -> K8sData:
        d = K8sData()
        try:
            from kubernetes import client, config as kconfig
            if self.cfg.in_cluster:
                kconfig.load_incluster_config()
            else:
                kconfig.load_kube_config(context=self.cfg.context)
            core = client.CoreV1Api()
            apps = client.AppsV1Api()
            ns = self.cfg.namespace
            all_ns = self.cfg.all_namespaces

            # 1순위: Deployment label 스캔으로 컴포넌트 동적 등록
            self._scan_deployments(apps, ns, all_ns)

            # 파드 수집 (Pod label로도 추가 등록됨)
            pod_items = (core.list_pod_for_all_namespaces().items if all_ns
                         else core.list_namespaced_pod(ns).items)
            for p in pod_items:
                comp = self._comp_of(p.metadata.labels or {}, p.metadata.name or "")
                statuses = p.status.container_statuses or []
                restarts = sum(cs.restart_count or 0 for cs in statuses)
                waiting = next((cs.state.waiting.reason for cs in statuses
                                if cs.state and cs.state.waiting and cs.state.waiting.reason), "")
                term = next((cs.last_state.terminated.reason for cs in statuses
                             if cs.last_state and cs.last_state.terminated
                             and cs.last_state.terminated.reason), "")
                ready = all((cs.ready for cs in statuses)) if statuses else False
                has_lim = True
                for c in p.spec.containers or []:
                    lim = (c.resources.limits if c.resources else None) or {}
                    if "cpu" not in lim and "memory" not in lim:
                        has_lim = False
                d.pods.append(PodInfo(
                    name=p.metadata.name, component=comp, phase=p.status.phase or "?",
                    ready=ready, restarts=restarts, waiting_reason=waiting or "",
                    term_reason=term or "", has_limits=has_lim, node=p.spec.node_name or "",
                    namespace=p.metadata.namespace or ns))
            try:
                ev_items = (core.list_event_for_all_namespaces().items if all_ns
                            else core.list_namespaced_event(ns).items)
                for e in ev_items:
                    if (e.type or "") == "Warning":
                        d.events.append(EventInfo(
                            e.reason or "", f"{e.involved_object.kind}/{e.involved_object.name}",
                            (e.message or "")[:160], e.count or 1))
                d.events.sort(key=lambda x: x.count, reverse=True)
                d.events = d.events[:15]
            except Exception:  # noqa: BLE001
                pass
            try:
                hpa_api = client.AutoscalingV2Api()
                hpa_items = (hpa_api.list_horizontal_pod_autoscaler_for_all_namespaces().items if all_ns
                             else hpa_api.list_namespaced_horizontal_pod_autoscaler(ns).items)
                for h in hpa_items:
                    d.hpas.append(HPAInfo(
                        h.metadata.name, self._comp_of(h.metadata.labels or {}),
                        h.status.current_replicas, h.status.desired_replicas,
                        h.spec.min_replicas, h.spec.max_replicas))
            except Exception:  # noqa: BLE001
                pass
            # PVC
            try:
                pvc_items = (core.list_persistent_volume_claim_for_all_namespaces().items if all_ns
                             else core.list_namespaced_persistent_volume_claim(ns).items)
                for pvc in pvc_items:
                    cap = ""
                    if pvc.status.capacity:
                        cap = pvc.status.capacity.get("storage", "")
                    d.pvcs.append(PVCInfo(
                        name=pvc.metadata.name, namespace=pvc.metadata.namespace or ns,
                        status=pvc.status.phase or "Unknown",
                        capacity=cap,
                        storage_class=pvc.spec.storage_class_name or ""))
            except Exception:  # noqa: BLE001
                pass
            # PDB
            try:
                policy_api = client.PolicyV1Api()
                pdb_items = (policy_api.list_pod_disruption_budget_for_all_namespaces().items if all_ns
                             else policy_api.list_namespaced_pod_disruption_budget(ns).items)
                for pdb in pdb_items:
                    d.pdbs.append(PDBInfo(
                        name=pdb.metadata.name,
                        component=self._comp_of(pdb.metadata.labels or {}),
                        min_available=str(pdb.spec.min_available) if pdb.spec.min_available is not None else None,
                        max_unavailable=str(pdb.spec.max_unavailable) if pdb.spec.max_unavailable is not None else None,
                        current_healthy=pdb.status.current_healthy,
                        desired_healthy=pdb.status.desired_healthy,
                        disruptions_allowed=pdb.status.disruptions_allowed))
            except Exception:  # noqa: BLE001
                pass
            # Nodes
            try:
                for node in core.list_node().items:
                    ready = False
                    bad_conds = []
                    for cond in (node.status.conditions or []):
                        if cond.type == "Ready" and cond.status == "True":
                            ready = True
                        elif cond.type != "Ready" and cond.status == "True":
                            bad_conds.append(cond.type)
                        elif cond.type == "Ready" and cond.status != "True":
                            bad_conds.append("NotReady")
                    taints = [f"{t.key}={t.value}:{t.effect}" for t in (node.spec.taints or [])]
                    alloc = node.status.allocatable or {}
                    d.nodes.append(NodeInfo(
                        name=node.metadata.name, ready=ready, conditions=bad_conds,
                        cpu_alloc=alloc.get("cpu", ""), mem_alloc=alloc.get("memory", ""),
                        taints=taints))
            except Exception:  # noqa: BLE001
                pass
            # Image 태그 검사
            try:
                img_pods = (core.list_pod_for_all_namespaces().items if all_ns
                            else core.list_namespaced_pod(ns).items)
                for pod in img_pods:
                    for c in (pod.spec.containers or []):
                        img = c.image or ""
                        has_latest = img.endswith(":latest")
                        has_no_tag = ":" not in img.split("/")[-1]
                        if has_latest or has_no_tag:
                            d.images.append(ImageInfo(
                                pod=pod.metadata.name, container=c.name,
                                image=img, has_latest_tag=has_latest, has_no_tag=has_no_tag))
            except Exception:  # noqa: BLE001
                pass
        except Exception as e:  # noqa: BLE001
            d.error = str(e).strip().splitlines()[0] if str(e) else "kubernetes 접근 실패"
        return d


# ──────────────────────────────────────────────────────────────────────────
# 계층 P — Prometheus
# ──────────────────────────────────────────────────────────────────────────
class PrometheusCollector:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self._headers = {}

    def _auth(self):
        if self.cfg.prometheus_aad:
            from azure.identity import DefaultAzureCredential
            tok = DefaultAzureCredential().get_token(
                "https://prometheus.monitor.azure.com/.default").token
            self._headers = {"Authorization": f"Bearer {tok}"}
        elif os.environ.get("PROM_BEARER"):
            self._headers = {"Authorization": f"Bearer {os.environ['PROM_BEARER']}"}

    def _q(self, query: str) -> list[dict]:
        import requests
        r = requests.get(self.cfg.prometheus_url.rstrip("/") + "/api/v1/query",
                         params={"query": query}, headers=self._headers, timeout=20)
        r.raise_for_status()
        j = r.json()
        if j.get("status") != "success":
            raise RuntimeError(j.get("error", "query failed"))
        return j["data"]["result"]

    def _q_range(self, query, start, end, step) -> list[dict]:
        import requests
        r = requests.get(self.cfg.prometheus_url.rstrip("/") + "/api/v1/query_range",
                         params={"query": query, "start": start, "end": end, "step": step},
                         headers=self._headers, timeout=30)
        r.raise_for_status()
        j = r.json()
        if j.get("status") != "success":
            raise RuntimeError(j.get("error", "query_range failed"))
        return j["data"]["result"]

    def _sub(self, q: str) -> str:
        return q.replace("$NS", self.cfg.namespace)

    @staticmethod
    def _by_pod(result: list[dict]) -> dict[str, float]:
        """Prometheus result를 namespace/pod key로 변환한다.

        -A 전체 네임스페이스 진단에서는 동일한 pod 이름이 서로 다른 namespace에
        존재할 수 있으므로 namespace/pod 형태가 안전하다.
        namespace 라벨이 없는 메트릭은 기존 호환을 위해 pod 이름만 사용한다.
        """
        out: dict[str, float] = {}
        for s in result:
            metric = s.get("metric", {})
            ns = metric.get("namespace", "")
            pod = metric.get("pod") or metric.get("pod_name") or metric.get("instance")
            if not pod:
                continue
            key = f"{ns}/{pod}" if ns else pod
            try:
                out[key] = float(s["value"][1])
            except Exception:  # noqa: BLE001
                pass
        return out

    def collect(self) -> PromData:
        d = PromData()
        if not self.cfg.prometheus_url:
            d.error = "--prometheus-url 미지정 → Prometheus 계층 생략."
            return d
        try:
            self._auth()
            ns = self.cfg.namespace
            all_ns = self.cfg.all_namespaces
            # -A 옵션이면 네임스페이스 필터 제거 (전체 조회)
            ns_filter = "" if all_ns else f',namespace="{ns}"'
            ns_filter_eq = "" if all_ns else f'{{namespace="{ns}"}}'
            d.enabled = True
            # cAdvisor 환경마다 container 라벨이 비어 있거나 기대한 이름과 다를 수 있다.
            # 따라서 pod 라벨 기준으로 집계하고, namespace 라벨을 함께 보존한다.
            ns_selector = '' if all_ns else f'namespace="{ns}",'
            selector = f'{{{ns_selector}pod!=""}}'
            limit_selector = f'{{{ns_selector}resource="memory",pod!=""}}'

            queries = {
                # pod 단위 throttle 비율. infra 컨테이너(POD)를 제외해 실제 컨테이너만 계산한다.
                "throttle_pct": (
                    f'100 * sum by (namespace, pod)(rate(container_cpu_cfs_throttled_periods_total{selector}[5m])) '
                    f'/ sum by (namespace, pod)(rate(container_cpu_cfs_periods_total{selector}[5m]))'),
                # limit이 있는 컨테이너는 limit 대비 사용률을 계산한다.
                "mem_pct": (
                    f'100 * sum by (namespace, pod)(container_memory_working_set_bytes{selector}) '
                    f'/ sum by (namespace, pod)(kube_pod_container_resource_limits{limit_selector})'
                ),
                # limit이 없는 static pod/control-plane 컴포넌트도 표시할 수 있도록 절대 메모리도 별도 수집한다.
                "mem_bytes": f'sum by (namespace, pod)(container_memory_working_set_bytes{selector})',
                "restarts": f'max by (namespace, pod)(kube_pod_container_status_restarts_total{ns_filter_eq})',
            }
            for key, q in queries.items():
                try:
                    d.per_pod[key] = self._by_pod(self._q(q))
                except Exception:  # noqa: BLE001
                    d.per_pod[key] = {}
            for key, q in {"lag": self.cfg.lag_query, "p99": self.cfg.latency_query,
                           "gpu": self.cfg.gpu_query}.items():
                try:
                    res = self._q(self._sub(q))
                    d.specialty[key] = float(res[0]["value"][1]) if res else None
                except Exception:  # noqa: BLE001
                    d.specialty[key] = None
            # Kubernetes control-plane 전용 신호. 메트릭이 없는 환경(AKS 관리형 control plane 등)에서는 None으로 둔다.
            cp_queries = {
                "apiserver_5xx_pct": (
                    '100 * sum(rate(apiserver_request_total{code=~"5.."}[5m])) '
                    '/ sum(rate(apiserver_request_total[5m]))'),
                "apiserver_p99_sec": (
                    'histogram_quantile(0.99, sum(rate(apiserver_request_duration_seconds_bucket[5m])) by (le))'),
                "etcd_fsync_p99_sec": (
                    'histogram_quantile(0.99, sum(rate(etcd_disk_wal_fsync_duration_seconds_bucket[5m])) by (le))'),
                "etcd_db_size_bytes": 'max(etcd_mvcc_db_total_size_in_bytes)',
                "scheduler_pending_pods": 'sum(kube_pod_status_phase{phase="Pending"})',
            }
            for key, q in cp_queries.items():
                try:
                    res = self._q(q)
                    d.specialty[key] = float(res[0]["value"][1]) if res else None
                except Exception:  # noqa: BLE001
                    d.specialty[key] = None
            now = int(dt.datetime.now().timestamp())
            try:
                # throttle 시계열: -A면 전체, 아니면 특정 ns
                if all_ns:
                    q_thr = ('100 * sum(rate(container_cpu_cfs_throttled_periods_total[5m]))'
                             ' / sum(rate(container_cpu_cfs_periods_total[5m]))')
                else:
                    q_thr = (f'100 * sum(rate(container_cpu_cfs_throttled_periods_total{{namespace="{ns}"}}[5m]))'
                             f' / sum(rate(container_cpu_cfs_periods_total{{namespace="{ns}"}}[5m]))')
                res = self._q_range(q_thr, now - self.cfg.hours * 3600, now, self.cfg.step_min * 60)
                if res:
                    d.series["throttle"] = MetricSeries(
                        "throttle", "%", [float(v[1]) for v in res[0]["values"]])
            except Exception:  # noqa: BLE001
                pass
            # PVC 사용률 (kubelet 메트릭)
            try:
                used_res = self._q('kubelet_volume_stats_used_bytes')
                cap_res  = self._q('kubelet_volume_stats_capacity_bytes')
                cap_map: dict[str, float] = {}
                for s in cap_res:
                    pvc = s["metric"].get("persistentvolumeclaim", "")
                    ns_pvc = s["metric"].get("namespace", "")
                    try:
                        cap_map[f"{ns_pvc}/{pvc}"] = float(s["value"][1])
                    except Exception:  # noqa: BLE001
                        pass
                for s in used_res:
                    pvc = s["metric"].get("persistentvolumeclaim", "")
                    ns_pvc = s["metric"].get("namespace", "")
                    key = f"{ns_pvc}/{pvc}"
                    try:
                        used = float(s["value"][1])
                        cap = cap_map.get(key, 0)
                        pct = round(used / cap * 100, 1) if cap > 0 else None
                        d.pvc_usage[key] = {"namespace": ns_pvc, "pvc": pvc,
                                            "used_pct": pct, "used_bytes": used, "cap_bytes": cap}
                    except Exception:  # noqa: BLE001
                        pass
            except Exception:  # noqa: BLE001
                pass
            # 노드별 CPU·메모리 요청 점유율
            try:
                cpu_req_res = self._q(
                    'sum by (node)(kube_pod_container_resource_requests{resource="cpu"})')
                cpu_alloc_res = self._q('kube_node_status_allocatable{resource="cpu"}')
                mem_req_res = self._q(
                    'sum by (node)(kube_pod_container_resource_requests{resource="memory"})')
                mem_alloc_res = self._q('kube_node_status_allocatable{resource="memory"}')
                cpu_alloc_map = {s["metric"].get("node",""):float(s["value"][1]) for s in cpu_alloc_res}
                mem_alloc_map = {s["metric"].get("node",""):float(s["value"][1]) for s in mem_alloc_res}
                cpu_req_map  = {s["metric"].get("node",""):float(s["value"][1]) for s in cpu_req_res}
                mem_req_map  = {s["metric"].get("node",""):float(s["value"][1]) for s in mem_req_res}
                for node in set(list(cpu_alloc_map.keys()) + list(mem_alloc_map.keys())):
                    ca = cpu_alloc_map.get(node, 0)
                    cr = cpu_req_map.get(node, 0)
                    ma = mem_alloc_map.get(node, 0)
                    mr = mem_req_map.get(node, 0)
                    d.node_usage[node] = {
                        "cpu_req_pct": round(cr/ca*100,1) if ca>0 else None,
                        "mem_req_pct": round(mr/ma*100,1) if ma>0 else None,
                        "cpu_req": cr, "cpu_alloc": ca,
                        "mem_req": mr, "mem_alloc": ma,
                    }
            except Exception:  # noqa: BLE001
                pass
            # 파드별 실제 CPU 사용량 (throttle 과 같이 capacity planning에 활용)
            try:
                cpu_actual_selector = selector
                cpu_actual_res = self._q(
                    f'sum by (namespace, pod)(rate(container_cpu_usage_seconds_total{cpu_actual_selector}[5m]))')
                for s in cpu_actual_res:
                    metric = s.get("metric", {})
                    ns_pod = metric.get("namespace", "")
                    pod = metric.get("pod", "")
                    if pod:
                        key = f"{ns_pod}/{pod}" if ns_pod else pod
                        try:
                            d.pod_cpu_actual[key] = round(float(s["value"][1]), 4)
                        except Exception:  # noqa: BLE001
                            pass
            except Exception:  # noqa: BLE001
                pass
        except Exception as e:  # noqa: BLE001
            d.error = str(e).strip().splitlines()[0]
        return d


# ──────────────────────────────────────────────────────────────────────────
# 계층 T — Tracing (App Insights / Log Analytics, OTel)
# ──────────────────────────────────────────────────────────────────────────
class TraceCollector:
    def __init__(self, cfg: Config):
        self.cfg = cfg

    def collect(self) -> TraceData:
        d = TraceData()
        if not self.cfg.appinsights_id:
            d.error = "--appinsights-id 미지정 → 트레이싱 계층 생략."
            return d
        try:
            from azure.identity import DefaultAzureCredential
            from azure.monitor.query import LogsQueryClient, LogsQueryStatus
            client = LogsQueryClient(DefaultAzureCredential())
            ws = self.cfg.trace_table_mode == "workspace"
            req_t, dep_t = ("AppRequests", "AppDependencies") if ws else ("requests", "dependencies")
            role, dur, ok = (("AppRoleName", "DurationMs", "Success") if ws
                             else ("cloud_RoleName", "duration", "success"))
            tgt = "Target" if ws else "target"

            q_comp = (f"{req_t} | summarize p95=percentile({dur},95), p99=percentile({dur},99), "
                      f"fail=100.0*countif({ok}==false)/count(), n=count() by {role}")
            q_hop = (f"{dep_t} | summarize p95=percentile({dur},95), p99=percentile({dur},99), "
                     f"fail=100.0*countif({ok}==false)/count(), n=count() by {role}, {tgt} "
                     f"| top 20 by p99 desc")

            span = dt.timedelta(hours=self.cfg.trace_hours)

            def run(query):
                r = client.query_resource(self.cfg.appinsights_id, query, timespan=span)
                if r.status != LogsQueryStatus.SUCCESS or not r.tables:
                    return [], []
                t = r.tables[0]
                cols = list(t.columns)
                return cols, [list(row) for row in t.rows]

            cols, rows = run(q_comp)
            ci = {c: i for i, c in enumerate(cols)}
            for row in rows:
                d.comp[str(row[ci[role]])] = CompLatency(
                    role=str(row[ci[role]]),
                    p95_ms=_f(row[ci["p95"]]), p99_ms=_f(row[ci["p99"]]),
                    fail_pct=_f(row[ci["fail"]]), count=int(row[ci["n"]] or 0))

            cols, rows = run(q_hop)
            ci = {c: i for i, c in enumerate(cols)}
            for row in rows:
                d.hops.append(HopStat(
                    source=str(row[ci[role]]), target=str(row[ci[tgt]]),
                    p95_ms=_f(row[ci["p95"]]), p99_ms=_f(row[ci["p99"]]),
                    fail_pct=_f(row[ci["fail"]]), count=int(row[ci["n"]] or 0)))
            d.enabled = True
        except Exception as e:  # noqa: BLE001
            d.error = str(e).strip().splitlines()[0]
        return d


def _f(v) -> Optional[float]:
    try:
        return round(float(v), 1)
    except Exception:  # noqa: BLE001
        return None


# ──────────────────────────────────────────────────────────────────────────
# 분석기
# ──────────────────────────────────────────────────────────────────────────
@dataclass
class CompRollup:
    pods: int = 0
    ready: int = 0
    restarts: int = 0
    no_limit_pods: int = 0
    throttle_max: Optional[float] = None
    mem_max: Optional[float] = None          # memory limit 대비 사용률(%)
    mem_bytes_max: Optional[float] = None    # limit이 없는 static pod용 절대 메모리(bytes)


class Analyzer:
    def __init__(self, k: K8sData, p: PromData, tr: TraceData, cfg: Config):
        self.k = k
        self.p = p
        self.tr = tr
        self.cfg = cfg
        self.findings: list[Finding] = []
        # COMPONENTS는 K8sCollector.collect() 후 이미 동적 등록 완료됨
        self.rollup: dict[str, CompRollup] = {c: CompRollup() for c in COMPONENTS + ["기타"]}

    def _rollups(self):
        pod_comp = {}
        for pod in self.k.pods:
            # pod.component가 rollup에 없으면 동적 추가 (Pod label에서 새로 발견된 경우)
            if pod.component not in self.rollup:
                self.rollup[pod.component] = CompRollup()
            r = self.rollup[pod.component]
            r.pods += 1
            r.ready += 1 if pod.ready else 0
            r.restarts += pod.restarts
            if not pod.has_limits:
                r.no_limit_pods += 1
            # Prometheus는 namespace/pod key를 우선 사용한다. 기존 호환을 위해 pod 이름만으로도 매핑한다.
            ns_key = f"{pod.namespace}/{pod.name}" if pod.namespace else pod.name
            pod_comp[ns_key] = pod.component
            pod_comp[pod.name] = pod.component
        for key, dest in (("throttle_pct", "throttle_max"), ("mem_pct", "mem_max"), ("mem_bytes", "mem_bytes_max")):
            for podname, val in self.p.per_pod.get(key, {}).items():
                comp = pod_comp.get(podname)
                plain_podname = podname.split("/", 1)[-1]
                if not comp:
                    comp = next((cp for prefix, cp in CONTROL_PLANE_PREFIXES.items() if plain_podname.startswith(prefix)), None)
                if not comp:
                    comp = next((c for c in COMPONENTS if plain_podname.startswith(c)), None)
                if not comp or comp not in self.rollup:
                    continue
                # NaN/Inf 방어: Prometheus가 0/0 연산 결과로 NaN 반환할 수 있음
                try:
                    import math as _m
                    if _m.isnan(val) or _m.isinf(val):
                        continue
                except (TypeError, ValueError):
                    continue
                cur = getattr(self.rollup[comp], dest)
                if cur is None or val > cur:
                    setattr(self.rollup[comp], dest, round(val, 1))

    def _trole(self, comp: str) -> Optional[CompLatency]:
        for role, cl in self.tr.comp.items():
            if comp in role.lower():
                return cl
        return None

    def run(self) -> list[Finding]:
        self._rollups()
        self._pod_health(); self._throttling(); self._memory(); self._limits()
        self._hpa(); self._specialty(); self._trace(); self._correlate()
        self._pvc(); self._pdb(); self._nodes(); self._control_plane(); self._images()
        if not self.findings:
            self.findings.append(Finding(SEV_OK, "전반", "특이 위험 없음",
                "수집된 신호에서 즉각적 임계치 초과가 없습니다.",
                "정기 진단을 스케줄링해 추세를 추적하세요."))
        order = {SEV_CRIT: 0, SEV_WARN: 1, SEV_INFO: 2, SEV_OK: 3}
        self.findings.sort(key=lambda f: order[f.severity])
        return self.findings

    def _pod_health(self):
        for pod in self.k.pods:
            kc = COMP_LABEL_KO.get(pod.component, pod.component)
            pod_ns = pod.namespace or self.cfg.namespace
            if pod.term_reason == "OOMKilled":
                self.findings.append(Finding(SEV_CRIT, kc, f"OOMKilled: {pod.name}",
                    "컨테이너가 메모리 한계 초과로 종료됐습니다.",
                    "메모리 limit 상향 또는 누수/배치 크기 점검.",
                    steps=[
                        f"kubectl describe pod {pod.name} -n {pod_ns}  # resources.limits.memory 확인",
                        f"kubectl logs {pod.name} -n {pod_ns} --previous  # 종료 직전 로그",
                        "# deployment/statefulset의 spec.containers[].resources.limits.memory 값을 현재의 1.3~1.5배로 상향",
                        "# 예: memory: 512Mi → memory: 768Mi",
                        "kubectl top pod -n " + self.cfg.namespace + "  # 실제 사용량 확인 후 조정",
                    ]))
            elif pod.waiting_reason in ("CrashLoopBackOff", "Error"):
                self.findings.append(Finding(SEV_CRIT, kc, f"{pod.waiting_reason}: {pod.name}",
                    f"반복 비정상 종료(재시작 {pod.restarts}회).",
                    "kubectl logs --previous 로 직전 종료 로그 확인.",
                    steps=[
                        f"kubectl logs {pod.name} -n {pod_ns} --previous  # 직전 종료 로그",
                        f"kubectl describe pod {pod.name} -n {pod_ns}  # Exit Code·Events 확인",
                        "# Exit Code 137=OOM, 1=앱오류, 139=SIGSEGV",
                        "# livenessProbe 설정이 너무 엄격한지 확인: initialDelaySeconds / failureThreshold",
                        "# spec.containers[].command / args 오류 여부 확인",
                    ]))
            elif pod.waiting_reason in ("ImagePullBackOff", "ErrImagePull"):
                self.findings.append(Finding(SEV_WARN, kc, f"{pod.waiting_reason}: {pod.name}",
                    "이미지 풀 실패.", "ACR 권한·태그·Private Endpoint DNS 확인.",
                    steps=[
                        f"kubectl describe pod {pod.name} -n {pod_ns}  # 정확한 이미지명·오류 메시지",
                        "az acr repository show-tags -n <ACR명> --repository <이미지명>  # 태그 존재 확인",
                        "kubectl get secret -n " + pod_ns + "  # imagePullSecret 존재 확인",
                        "# AKS → ACR 연결: az aks update -g <RG> -n <AKS> --attach-acr <ACR명>",
                    ]))
            elif pod.phase == "Pending":
                # ── Pending 상세 분석: events + nodes 활용 ──────────────
                # 해당 pod의 FailedScheduling 이벤트 메시지 추출
                sched_msgs = [
                    e.message for e in self.k.events
                    if pod.name in e.obj and "FailedScheduling" in e.reason
                ]
                # 원인 분류
                causes = []
                raw_msg = " ".join(sched_msgs).lower()
                if "insufficient cpu" in raw_msg:
                    causes.append("CPU 부족")
                if "insufficient memory" in raw_msg:
                    causes.append("메모리 부족")
                if "node(s) had taint" in raw_msg or "taint" in raw_msg:
                    causes.append("Taint 불일치")
                if "node selector" in raw_msg or "nodeselector" in raw_msg:
                    causes.append("NodeSelector 불일치")
                if "gpu" in raw_msg or "nvidia" in raw_msg:
                    causes.append("GPU 노드 부재")
                if "unschedulable" in raw_msg:
                    causes.append("노드 unschedulable")
                if not causes and sched_msgs:
                    causes.append("스케줄 불가")
                elif not causes:
                    causes.append("원인 미확인 (describe 필요)")

                # 노드 현황 요약
                total_nodes = len(self.k.nodes)
                ready_nodes = sum(1 for n in self.k.nodes if n.ready)
                node_taints = []
                for n in self.k.nodes:
                    if n.taints:
                        node_taints.append(f"{n.name}: {', '.join(n.taints)}")

                cause_str = " / ".join(causes)
                node_summary = f"노드 현황: 전체 {total_nodes}개 중 Ready {ready_nodes}개"
                if node_taints:
                    node_summary += f" | Taint 있는 노드: {'; '.join(node_taints[:3])}"

                event_detail = ""
                if sched_msgs:
                    event_detail = f" | 스케줄러 메시지: {sched_msgs[0][:120]}"

                steps = [
                    f"kubectl describe pod {pod.name} -n {pod_ns}  # Events > FailedScheduling 상세",
                    f"kubectl get nodes -o wide  # 현재 노드 수: {total_nodes}개 / Ready: {ready_nodes}개",
                ]
                if "CPU 부족" in causes or "메모리 부족" in causes:
                    steps.append("kubectl describe nodes | grep -A8 'Allocated resources'  # 노드별 잔여 리소스 확인")
                    steps.append("# 해결: 노드풀 스케일 아웃 → az aks scale -g <RG> -n <AKS> --node-count <N> --nodepool-name <pool>")
                    steps.append("# 또는 Cluster Autoscaler 활성화: az aks update --enable-cluster-autoscaler --min-count 1 --max-count 5")
                if "Taint 불일치" in causes:
                    steps.append("kubectl describe nodes | grep -A3 Taints  # 노드 Taint 목록 확인")
                    steps.append("# 해결: pod spec에 tolerations 추가 또는 kubectl taint nodes <node> <key>-  (Taint 제거)")
                if "GPU 노드 부재" in causes:
                    steps.append("kubectl get nodes -l accelerator=nvidia  # GPU 노드 존재 확인")
                    steps.append("# 해결: az aks nodepool add --name gpupool --node-vm-size Standard_NC6s_v3 --node-count 1")
                if "NodeSelector 불일치" in causes:
                    steps.append(f"kubectl get pod {pod.name} -n {self.cfg.namespace} -o jsonpath='{{.spec.nodeSelector}}'  # 요구 레이블")
                    steps.append("kubectl get nodes --show-labels  # 노드 레이블 목록")

                self.findings.append(Finding(
                    SEV_WARN, kc, f"Pending: {pod.name}",
                    f"스케줄 실패 — 원인: {cause_str}. {node_summary}.{event_detail}",
                    f"원인({cause_str}) 해소 후 pod가 자동 재스케줄됩니다.",
                    steps=steps))
            elif pod.restarts >= 5:
                self.findings.append(Finding(SEV_WARN, kc, f"잦은 재시작: {pod.name} ({pod.restarts}회)",
                    "불안정 신호.", "재시작 원인(OOM/probe/예외)을 추적하세요.",
                    steps=[
                        f"kubectl logs {pod.name} -n {pod_ns} --previous",
                        f"kubectl describe pod {pod.name} -n {pod_ns}  # Last State.Reason",
                        "# readinessProbe / livenessProbe의 initialDelaySeconds를 늘려보세요",
                    ]))

    def _throttling(self):
        for c in [c for c in COMPONENTS if c in self.rollup]:
            t = self.rollup[c].throttle_max
            if t is not None and t >= 25:
                sev = SEV_WARN if t < 50 else SEV_CRIT
                label = COMP_LABEL_KO.get(c, c)
                self.findings.append(Finding(sev, label, f"CPU throttling {t}%",
                    "CPU limit 에 걸려 실행이 깎임(리소스 메트릭 사각지대).",
                    "CPU limit 상향/제거 검토, HPA 임계 재조정.",
                    steps=[
                        f"# {c} Deployment의 spec.containers[].resources.limits.cpu 값 상향",
                        f"kubectl edit deployment {c} -n {self.cfg.namespace}",
                        "# 예: limits.cpu: 500m → limits.cpu: 1000m  (또는 limit 제거 후 HPA로 수평 확장)",
                        "# throttle이 지속적이면 limit 제거 + HPA targetCPUUtilizationPercentage: 60 설정 권장",
                        f"kubectl get hpa -n {self.cfg.namespace}  # HPA 현황 확인",
                    ]))

    def _memory(self):
        for c in [c for c in COMPONENTS if c in self.rollup]:
            m = self.rollup[c].mem_max
            if m is not None and m >= 90:
                label = COMP_LABEL_KO.get(c, c)
                self.findings.append(Finding(SEV_WARN, label,
                    f"메모리 사용률 {m}% (limit 대비)", "OOMKill 임박 신호.",
                    "memory limit 상향 또는 사용 최적화.",
                    steps=[
                        f"kubectl top pod -n {self.cfg.namespace} --sort-by=memory  # 실제 사용량 순위",
                        f"# {c} Deployment의 resources.limits.memory를 현재의 1.3배 이상으로 상향",
                        f"kubectl edit deployment {c} -n {self.cfg.namespace}",
                        "# 예: limits.memory: 1Gi → limits.memory: 1.5Gi",
                        "# 근본 해결: 배치 크기 줄이기, 메모리 캐시 TTL 단축, 힙 덤프 분석",
                    ]))

    def _limits(self):
        no_lim = [p for p in self.k.pods if not p.has_limits and p.component in COMPONENTS]
        if no_lim:
            comps = ", ".join(sorted({COMP_LABEL_KO.get(p.component, p.component) for p in no_lim}))
            self.findings.append(Finding(SEV_INFO, "구성",
                f"리소스 limit 미설정 파드 {len(no_lim)}개 ({comps})",
                "경합·예측 불가 throttling·OOM 위험 증가.",
                "requests/limits 설정. Azure Policy for AKS 로 강제 가능.",
                steps=[
                    "# 각 Deployment의 spec.containers[].resources 에 아래 추가:",
                    "# resources:",
                    "#   requests:",
                    "#     cpu: 100m",
                    "#     memory: 256Mi",
                    "#   limits:",
                    "#     cpu: 500m",
                    "#     memory: 512Mi",
                    "# Azure Policy로 강제: az policy assignment create --policy 'Container CPU and memory resource limits should not exceed ...'",
                ]))

    def _hpa(self):
        for h in self.k.hpas:
            if h.maxr and h.current and h.current >= h.maxr:
                self.findings.append(Finding(SEV_WARN, COMP_LABEL_KO.get(h.component, h.component),
                    f"HPA 최대치 도달: {h.name} ({h.current}/{h.maxr})",
                    "스케일 상한 도달 — 추가 부하 흡수 여력 없음.",
                    "maxReplicas 상향 또는 노드풀 한도·쿼터 확인.",
                    steps=[
                        f"kubectl edit hpa {h.name} -n {self.cfg.namespace}",
                        f"# spec.maxReplicas: {h.maxr} → {(h.maxr or 0) * 2}  (현재의 2배로 상향 검토)",
                        "kubectl get nodes  # 노드 수가 충분한지 확인",
                        "az aks nodepool scale -g <RG> -n <AKS> --nodepool-name <pool> --node-count <n>  # 노드 확장",
                        "az aks nodepool update --enable-cluster-autoscaler --min-count 2 --max-count 10 ...  # CA 활성화",
                    ]))

    def _specialty(self):
        lag = self.p.specialty.get("lag")
        if lag is not None and lag >= 10000:
            self.findings.append(Finding(SEV_WARN if lag < 100000 else SEV_CRIT, "Aggregator",
                f"Consumer lag {int(lag):,}", "인입을 소비가 못 따라감(백프레셔).",
                "레플리카/파티션 병렬도 상향, KEDA 임계 점검.",
                steps=[
                    "kubectl scale deployment aggregator --replicas=<현재+2> -n " + self.cfg.namespace,
                    "# Kafka 파티션 수 확인: kafka-topics.sh --describe --topic <topic>",
                    "# 파티션 수 < 레플리카 수이면 파티션 확장 필요 (kafka-topics.sh --alter --partitions <n>)",
                    "# KEDA ScaledObject의 threshold 값을 현재 lag 기준으로 재조정",
                ]))
        gpu = self.p.specialty.get("gpu")
        if gpu is not None and gpu < 30:
            self.findings.append(Finding(SEV_INFO, "Trainer", f"GPU 평균 사용률 {gpu:.0f}%",
                "GPU 저활용 — 데이터 파이프라인 병목 가능.",
                "데이터 로더 병렬화/프리페치, 배치 크기·mixed precision 검토.",
                steps=[
                    "# DataLoader num_workers 를 CPU 코어 수의 50~75%로 설정",
                    "# pin_memory=True, prefetch_factor=2 설정",
                    "# 배치 크기를 GPU 메모리 80% 점유 수준까지 상향",
                    "# torch.cuda.amp.autocast() 로 mixed precision 활성화",
                    "# nvidia-smi dmon -s u  # GPU 실시간 사용률 모니터링",
                ]))

    def _trace(self):
        if not self.tr.enabled:
            return
        for role, cl in self.tr.comp.items():
            comp = next((c for c in COMPONENTS if c in role.lower()), None)
            kc = COMP_LABEL_KO.get(comp, role)
            if cl.p99_ms and cl.p99_ms >= 500:
                self.findings.append(Finding(SEV_WARN, kc, f"요청 p99 {cl.p99_ms:.0f}ms ({role})",
                    "서버측 처리 지연이 큽니다(트레이싱).",
                    "App Insights end-to-end transaction 으로 느린 span 분해.",
                    steps=[
                        "# Azure Portal → App Insights → 성능 → 느린 요청 클릭 → End-to-end 트랜잭션",
                        f"# 가장 느린 span의 대상(DB/외부 API/내부 서비스) 특정",
                        "# 해당 의존성의 타임아웃·연결 풀·인덱스 설정 점검",
                    ]))
            if cl.fail_pct and cl.fail_pct >= 5:
                self.findings.append(Finding(SEV_WARN, kc, f"요청 실패율 {cl.fail_pct:.1f}% ({role})",
                    "실패 비율이 높습니다.", "실패 span 의 예외/의존성 오류를 확인하세요.",
                    steps=[
                        "# Azure Portal → App Insights → 실패 → 예외 유형별 분류",
                        "# 서킷 브레이커 패턴 적용 검토 (Polly / Resilience4j)",
                        "# 재시도 정책: 지수 백오프 + 최대 3회",
                    ]))
        if self.tr.hops:
            slow = max(self.tr.hops, key=lambda h: h.p99_ms or 0)
            if slow.p99_ms and slow.p99_ms >= 300:
                self.findings.append(Finding(SEV_INFO, "흐름",
                    f"최대 지연 hop: {slow.source}→{slow.target} (p99 {slow.p99_ms:.0f}ms)",
                    "컴포넌트 간 가장 느린 호출 구간.",
                    "해당 의존성(대상 서비스/DB/외부 API) 지연 원인 추적.",
                    steps=[
                        f"# {slow.source} → {slow.target} 구간 집중 분석",
                        "# App Insights dependency 테이블에서 느린 호출 샘플 확인",
                        "# 연결 풀 크기, 타임아웃 설정, 대상 서비스 부하 확인",
                    ]))
            for h in self.tr.hops:
                if h.fail_pct and h.fail_pct >= 5:
                    self.findings.append(Finding(SEV_WARN, "흐름",
                        f"hop 실패율 {h.fail_pct:.1f}%: {h.source}→{h.target}",
                        "호출 구간 실패가 잦습니다.",
                        "재시도·타임아웃·서킷브레이커 설정과 대상 상태 확인.",
                        steps=[
                            f"# {h.source} 서비스의 HTTP 클라이언트 타임아웃 설정 확인",
                            f"# {h.target} 서비스 상태·로그 확인",
                            "# 네트워크 정책(NetworkPolicy)이 해당 포트를 차단하는지 확인",
                        ]))

    def _correlate(self):
        # 모든 컴포넌트 중 p99 지연 + throttle 동시 발생 감지 (하드코딩 제거)
        for c in COMPONENTS:
            if c not in self.rollup:
                continue
            cl = self._trole(c)
            thr = self.rollup[c].throttle_max
            if cl and cl.p99_ms and cl.p99_ms >= 500 and thr is not None and thr >= 25:
                label = COMP_LABEL_KO.get(c, c)
                self.findings.append(Finding(SEV_CRIT, label,
                    f"추론 지연 + throttling 동반 (p99 {cl.p99_ms:.0f}ms / throttle {thr}%)",
                    "트레이싱의 꼬리 지연과 Prometheus 의 CPU throttling 이 같은 컴포넌트에서 동시 발생 — "
                    "지연 원인이 CPU 한계일 가능성이 큽니다(메트릭·트레이스 상관).",
                    f"{label} CPU limit 상향/제거를 1순위로. 적용 후 p99 재측정으로 효과 확인.",
                    steps=[
                        f"kubectl edit deployment {c} -n {self.cfg.namespace}",
                        "# resources.limits.cpu 값을 현재의 2배로 상향 또는 제거",
                        "# 적용 후 5분 대기 → 아래 명령으로 throttle 재확인",
                        f"kubectl top pod -n {self.cfg.namespace} -l {self.cfg.component_label}={c}",
                        "# p99 개선 없으면 추론 배치 크기 축소 또는 모델 경량화 검토",
                    ]))

    # ── 신규: PVC 스토리지 ─────────────────────────────────────────────────
    def _pvc(self):
        for pvc in self.k.pvcs:
            if pvc.status == "Lost":
                self.findings.append(Finding(SEV_CRIT, "스토리지",
                    f"PVC Lost: {pvc.name}",
                    "PVC가 연결된 PV를 찾지 못합니다. 데이터 접근 불가 상태.",
                    "PV 상태 확인 후 재바인딩 또는 복구 필요.",
                    steps=[
                        f"kubectl describe pvc {pvc.name} -n {self.cfg.namespace}",
                        "kubectl get pv  # 연결된 PV 상태 확인",
                        "# PV가 Released 상태이면: kubectl edit pv <pv명> → claimRef 삭제 후 재바인딩",
                        "# Azure Disk: az disk show -n <disk명> -g <RG>  # 디스크 존재 확인",
                    ]))
            elif pvc.status == "Pending":
                self.findings.append(Finding(SEV_WARN, "스토리지",
                    f"PVC Pending: {pvc.name}",
                    "PVC가 아직 바인딩되지 않았습니다.",
                    "StorageClass·용량·AZ 일치 여부 확인.",
                    steps=[
                        f"kubectl describe pvc {pvc.name} -n {self.cfg.namespace}  # Events 확인",
                        f"kubectl get storageclass {pvc.storage_class}  # StorageClass 존재 확인",
                        "# WaitForFirstConsumer 정책이면 파드가 먼저 스케줄돼야 PVC 바인딩됨",
                        "# 용량 할당량 초과 시: az quota show --scope /subscriptions/<sub> ...",
                    ]))

    # ── 신규: PDB ─────────────────────────────────────────────────────────
    def _pdb(self):
        for pdb in self.k.pdbs:
            if pdb.disruptions_allowed is not None and pdb.disruptions_allowed == 0:
                self.findings.append(Finding(SEV_WARN, "가용성",
                    f"PDB 중단 허용 0: {pdb.name}",
                    f"현재 허용 중단 수가 0입니다 (healthy: {pdb.current_healthy}/{pdb.desired_healthy}). "
                    "노드 업그레이드·유지보수 시 드레인이 차단될 수 있습니다.",
                    "minAvailable/maxUnavailable 재조정 또는 레플리카 수 증가.",
                    steps=[
                        f"kubectl describe pdb {pdb.name} -n {self.cfg.namespace}",
                        "# 레플리카가 1개이면 PDB가 항상 차단 → 레플리카 최소 2개로 증가 필요",
                        f"kubectl edit pdb {pdb.name} -n {self.cfg.namespace}",
                        "# minAvailable: 1 → maxUnavailable: 1 로 변경하면 유지보수 용이",
                        "# AKS 노드 업그레이드: az aks upgrade 시 PDB 차단으로 멈출 수 있음",
                    ]))
            if (pdb.min_available and pdb.min_available == str(pdb.desired_healthy)):
                self.findings.append(Finding(SEV_INFO, "가용성",
                    f"PDB minAvailable = 전체 파드 수: {pdb.name}",
                    "minAvailable이 전체 desired 수와 같아 중단이 전혀 허용되지 않습니다.",
                    "노드 유지보수 시 자동 드레인 차단 위험.",
                    steps=[
                        f"kubectl edit pdb {pdb.name} -n {self.cfg.namespace}",
                        "# minAvailable: N → N-1 로 줄이거나, maxUnavailable: 1 로 전환",
                    ]))

    # ── 신규: Node 상태 ───────────────────────────────────────────────────
    def _nodes(self):
        for node in self.k.nodes:
            if not node.ready or "NotReady" in node.conditions:
                self.findings.append(Finding(SEV_CRIT, "노드",
                    f"Node NotReady: {node.name}",
                    f"노드가 Ready 상태가 아닙니다. 조건: {', '.join(node.conditions) or 'NotReady'}",
                    "노드 상태 및 kubelet 로그 확인.",
                    steps=[
                        f"kubectl describe node {node.name}  # Conditions·Events 확인",
                        f"kubectl get pods -A --field-selector=spec.nodeName={node.name}  # 영향받는 파드",
                        "# VM 상태: az vm show -g <RG> -n <node명>",
                        "# kubelet 로그: journalctl -u kubelet -n 100 (노드 SSH 접속 후)",
                        "# 복구 안되면: kubectl drain " + node.name + " --ignore-daemonsets --delete-emptydir-data",
                        f"# 노드 재생성: az aks nodepool upgrade 또는 노드 삭제 후 CA 재생성",
                    ]))
            bad = [c for c in node.conditions if c not in ("NotReady",)]
            for cond in bad:
                self.findings.append(Finding(SEV_WARN, "노드",
                    f"Node 비정상 조건 [{cond}]: {node.name}",
                    f"노드에 {cond} 조건이 활성화됐습니다.",
                    "노드 리소스(디스크/메모리/PID) 상태 점검.",
                    steps=[
                        f"kubectl describe node {node.name}  # {cond} 조건 상세",
                        "# DiskPressure: 노드 디스크 정리 또는 확장",
                        "# MemoryPressure: 파드 메모리 limit 조정 또는 노드 확장",
                        "# PIDPressure: 파드 수 또는 프로세스 수 감소",
                    ]))

    # ── 신규: Kubernetes control-plane 전용 진단 ───────────────────────
    def _control_plane(self):
        api_5xx = self.p.specialty.get("apiserver_5xx_pct")
        if api_5xx is not None and api_5xx >= 1:
            self.findings.append(Finding(SEV_WARN if api_5xx < 5 else SEV_CRIT, "Kube-apiserver",
                f"API Server 5xx 비율 {api_5xx:.1f}%",
                "Kubernetes API 서버 오류 응답 비율이 높습니다.",
                "apiserver 로그·Audit·요청량 급증 여부를 확인하세요.",
                steps=[
                    "kubectl get --raw /metrics | grep apiserver_request_total  # self-managed 환경",
                    "Prometheus: rate(apiserver_request_total{code=~'5..'}[5m])",
                    "AKS 관리형 control plane이면 Azure Portal → AKS → Diagnose and solve problems / Control plane logs 확인",
                ]))
        api_p99 = self.p.specialty.get("apiserver_p99_sec")
        if api_p99 is not None and api_p99 >= 1:
            self.findings.append(Finding(SEV_WARN if api_p99 < 3 else SEV_CRIT, "Kube-apiserver",
                f"API Server p99 지연 {api_p99:.2f}s",
                "Kubernetes API 요청 꼬리 지연이 큽니다.",
                "대량 watch/list 클라이언트, admission webhook 지연, etcd 지연을 함께 점검하세요.",
                steps=[
                    "Prometheus: histogram_quantile(0.99, sum(rate(apiserver_request_duration_seconds_bucket[5m])) by (le))",
                    "kubectl get events -A --sort-by=.lastTimestamp | tail -50",
                    "Admission Webhook timeout/failurePolicy 설정 확인",
                ]))
        fsync = self.p.specialty.get("etcd_fsync_p99_sec")
        if fsync is not None and fsync >= 0.05:
            self.findings.append(Finding(SEV_WARN if fsync < 0.2 else SEV_CRIT, "Etcd",
                f"etcd WAL fsync p99 {fsync:.3f}s",
                "etcd 디스크 동기화 지연이 높습니다. API Server 지연으로 이어질 수 있습니다.",
                "디스크 I/O, control-plane 부하, snapshot/compaction 상태를 점검하세요.",
                steps=[
                    "Prometheus: histogram_quantile(0.99, sum(rate(etcd_disk_wal_fsync_duration_seconds_bucket[5m])) by (le))",
                    "Prometheus: etcd_mvcc_db_total_size_in_bytes",
                    "AKS 관리형 etcd는 직접 조치가 제한되므로 Azure 지원/진단 로그 확인",
                ]))
        pending = self.p.specialty.get("scheduler_pending_pods")
        if pending is not None and pending >= 1:
            self.findings.append(Finding(SEV_INFO, "Kube-scheduler",
                f"Pending Pod {int(pending)}개",
                "스케줄러가 배치하지 못한 Pod가 있습니다.",
                "Pending Pod의 Events에서 리소스 부족, taint, nodeSelector, PVC 바인딩 대기 여부를 확인하세요.",
                steps=[
                    "kubectl get pod -A --field-selector=status.phase=Pending",
                    "kubectl describe pod <pod> -n <namespace>  # FailedScheduling 이벤트 확인",
                    "kubectl describe nodes | grep -A8 'Allocated resources'",
                ]))

    # ── 신규: 이미지 취약점 경고 (태그 검사) ─────────────────────────────
    def _images(self):
        if not self.k.images:
            return
        latest_imgs = [i for i in self.k.images if i.has_latest_tag]
        notag_imgs = [i for i in self.k.images if i.has_no_tag]
        if latest_imgs:
            names = ", ".join(f"{i.pod}/{i.container}" for i in latest_imgs[:3])
            self.findings.append(Finding(SEV_WARN, "보안",
                f":latest 태그 사용 {len(latest_imgs)}개 컨테이너 ({names}{'...' if len(latest_imgs) > 3 else ''})",
                ":latest 태그는 재배포 시 의도치 않은 버전이 풀릴 수 있어 재현 불가 장애의 원인이 됩니다.",
                "모든 이미지에 고정 버전 태그(예: v1.2.3, SHA digest) 사용.",
                steps=[
                    "# 각 Deployment의 spec.containers[].image 에서 :latest → :<version> 으로 변경",
                    "# SHA digest 고정(가장 안전): image: myacr.azurecr.io/app@sha256:<digest>",
                    "az acr repository show-manifests -n <ACR> --repository <이미지>  # digest 확인",
                    "# Azure Defender for Containers 활성화: az security pricing create -n Containers --tier Standard",
                ]))
        if notag_imgs:
            names = ", ".join(f"{i.pod}/{i.container}" for i in notag_imgs[:3])
            self.findings.append(Finding(SEV_INFO, "보안",
                f"태그 없는 이미지 {len(notag_imgs)}개 ({names}{'...' if len(notag_imgs) > 3 else ''})",
                "태그 없는 이미지는 :latest와 동일한 위험을 가집니다.",
                "명시적 태그 또는 SHA digest 사용.",
                steps=[
                    "kubectl get pods -n " + self.cfg.namespace + " -o jsonpath='{range .items[*]}{.spec.containers[*].image}{\"\\n\"}{end}'",
                    "# 각 이미지에 명시적 버전 태그 추가",
                ]))


def health_score(findings: list[Finding]) -> int:
    return max(0, min(100, 100 - sum(SEV_WEIGHT.get(f.severity, 0) for f in findings)))


# ──────────────────────────────────────────────────────────────────────────
# Blob 영속화 (CronJob 운영용)
# ──────────────────────────────────────────────────────────────────────────
class BlobStore:
    def __init__(self, cfg: Config):
        from azure.identity import DefaultAzureCredential
        from azure.storage.blob import BlobServiceClient
        self.cfg = cfg
        svc = BlobServiceClient(cfg.blob_account_url, credential=DefaultAzureCredential())
        self.cc = svc.get_container_client(cfg.blob_container)
        try:
            self.cc.create_container()
        except Exception:  # noqa: BLE001 — 이미 존재
            pass

    def list_history(self, ns):
        return sorted(b.name for b in self.cc.list_blobs(name_starts_with=f"history/{ns}/"))

    def read_json(self, name):
        return json.loads(self.cc.download_blob(name).readall())

    def write_json(self, name, obj):
        self.cc.upload_blob(name, json.dumps(obj, ensure_ascii=False, indent=2).encode(),
                            overwrite=True)

    def upload_report(self, local_path, ns, ts):
        name = f"reports/{ns}/{ts}.html"
        for n in (name, f"reports/{ns}/latest.html"):
            with open(local_path, "rb") as fh:
                self.cc.upload_blob(n, fh, overwrite=True)
        return name


# ──────────────────────────────────────────────────────────────────────────
# Baseline 이력 (로컬 FS 또는 Blob)
# ──────────────────────────────────────────────────────────────────────────
def _safe(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", s or "demo")


def load_baseline(cfg: Config, store: Optional[BlobStore]) -> Optional[dict]:
    if not cfg.history:
        return None
    try:
        if store:
            names = store.list_history(cfg.namespace)
            return store.read_json(names[-1]) if names else None
        files = sorted(glob.glob(os.path.join(cfg.history_dir, f"{_safe(cfg.namespace)}__*.json")))
        if not files:
            return None
        with open(files[-1], encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:  # noqa: BLE001
        return None


def save_snapshot(cfg: Config, score: int, rollup, store: Optional[BlobStore]):
    if not cfg.history or cfg.demo:
        return
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    snap = {"schema_version": 1, "namespace": cfg.namespace,
            "generated_at": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "health_score": score,
            "components": {c: {"restarts": rollup[c].restarts,
                               "throttle_max": rollup[c].throttle_max}
                           for c in COMPONENTS if c in rollup}}
    try:
        if store:
            store.write_json(f"history/{cfg.namespace}/{ts}.json", snap)
        else:
            os.makedirs(cfg.history_dir, exist_ok=True)
            with open(os.path.join(cfg.history_dir, f"{_safe(cfg.namespace)}__{ts}.json"),
                      "w", encoding="utf-8") as fh:
                json.dump(snap, fh, ensure_ascii=False, indent=2)
    except Exception:  # noqa: BLE001
        pass


# ──────────────────────────────────────────────────────────────────────────
# HTML 리포터
# ──────────────────────────────────────────────────────────────────────────
def _esc(v: Any) -> str:
    return html.escape("" if v is None else str(v))


def _spark(values, color, w=600, h=60) -> str:
    pts = [v for v in values if v is not None]
    if len(pts) < 2:
        return '<span class="muted">데이터 없음</span>'
    lo, hi = min(pts), max(pts)
    span = (hi - lo) or 1
    n = len(values)
    coords = []
    for i, v in enumerate(values):
        if v is None:
            continue
        x = i / (n - 1) * (w - 4) + 2
        y = h - 2 - (v - lo) / span * (h - 6)
        coords.append(f"{x:.1f},{y:.1f}")
    return (f'<svg class="spark" viewBox="0 0 {w} {h}" preserveAspectRatio="none">'
            f'<polyline points="{" ".join(coords)}" fill="none" stroke="{color}" stroke-width="1.6"/></svg>')


def render_html(cfg, k: K8sData, p: PromData, tr: TraceData, rollup, findings,
                baseline, generated_at) -> str:
    health = health_score(findings)   # 내부 건강 점수 (100=완벽, 0=최악) — history 호환용
    risk   = 100 - health             # 화면 표시용 위험도 점수 (0=안전, 100=최악)
    nc = sum(1 for f in findings if f.severity == SEV_CRIT)
    nw = sum(1 for f in findings if f.severity == SEV_WARN)
    ni = sum(1 for f in findings if f.severity == SEV_INFO)
    # 위험도 라벨: 높을수록 위험
    if nc > 0:
        risk_label, risk_cls = "위험", SEV_CRIT
    elif nw > 0:
        risk_label, risk_cls = "주의", SEV_WARN
    elif risk > 0:
        risk_label, risk_cls = "정보", SEV_INFO
    else:
        risk_label, risk_cls = "안전", SEV_OK

    def _steps_html(steps, fid):
        if not steps:
            return ""
        items = "".join(f'<li>{_esc(s)}</li>' for s in steps)
        return (f'<button class="steps-btn" onclick="toggleSteps(\'{fid}\')">'
                f'▶ 상세 조치 단계 보기</button>'
                f'<ol class="steps-list" id="{fid}" style="display:none">{items}</ol>')

    finding_cards = ""
    for i, f in enumerate(findings):
        fid = f"steps_{i}"
        finding_cards += f"""
      <article class="finding sev-{f.severity}">
        <div class="finding-head"><span class="chip chip-{f.severity}">{SEV_LABEL[f.severity]}</span>
          <span class="cat">{_esc(COMP_LABEL_KO.get(f.component, f.component))}</span><h3>{_esc(f.title)}</h3></div>
        <p class="detail">{_esc(f.detail)}</p>
        <div class="reco"><span class="reco-label">권장 조치</span>{_esc(f.recommendation)}</div>
        {_steps_html(f.steps, fid)}
      </article>"""

    def _fmt_bytes(b):
        if b is None or b == 0:
            return "—"
        try:
            b = float(b)
        except Exception:
            return "—"
        for unit in ["B", "Ki", "Mi", "Gi", "Ti"]:
            if b < 1024 or unit == "Ti":
                return f"{b:.1f}{unit}" if unit != "B" else f"{b:.0f}B"
            b /= 1024
        return f"{b:.1f}Ti"

    # 컴포넌트 색상: 순환 팔레트 (컴포넌트 수에 무관)
    _CARD_COLORS = ["#0078D4", "#8661c5", "#107c10", "#c8362f", "#b07b00", "#0a6cbd", "#5a4b35", "#1a7a6e"]
    comp_cards = ""
    display_comps = [c for c in COMPONENTS if c in rollup and c != "기타"]
    for idx, c in enumerate(display_comps):
        r = rollup[c]
        color = _CARD_COLORS[idx % len(_CARD_COLORS)]
        label = COMP_LABEL_KO.get(c, c)
        thr = f"{r.throttle_max}%" if r.throttle_max is not None else "—"
        if r.mem_max is not None:
            mem = f"{r.mem_max}%"
        elif r.mem_bytes_max is not None:
            mem = f"{_fmt_bytes(r.mem_bytes_max)}"
        else:
            mem = "—"
        comp_cards += f"""
        <div class="comp-card" style="border-top:3px solid {color}">
          <div class="comp-name">{_esc(label)}</div>
          <div class="comp-grid">
            <div><span>파드</span><b>{r.ready}/{r.pods}</b></div>
            <div><span>재시작</span><b>{r.restarts}</b></div>
            <div><span>throttle</span><b>{thr}</b></div>
            <div><span>mem/limit</span><b>{mem}</b></div>
          </div>
        </div>"""

    # 트레이싱
    if tr.enabled:
        crows = "".join(
            f"<tr><td>{_esc(c.role)}</td><td>{c.p95_ms or '—'}</td><td>{c.p99_ms or '—'}</td>"
            f"<td>{('%.1f'%c.fail_pct) if c.fail_pct is not None else '—'}</td><td>{c.count}</td></tr>"
            for c in tr.comp.values())
        hrows = "".join(
            f"<tr><td>{_esc(h.source)} → {_esc(h.target)}</td><td>{h.p95_ms or '—'}</td>"
            f"<td>{h.p99_ms or '—'}</td>"
            f"<td>{('%.1f'%h.fail_pct) if h.fail_pct is not None else '—'}</td><td>{h.count}</td></tr>"
            for h in tr.hops)
        trace_html = (
            '<div class="block"><h3>컴포넌트별 요청 latency (ms)</h3>'
            f'<table><thead><tr><th>role</th><th>p95</th><th>p99</th><th>fail %</th><th>n</th>'
            f'</tr></thead><tbody>{crows or "<tr><td colspan=5 class=muted>데이터 없음</td></tr>"}</tbody></table></div>'
            '<div class="block"><h3>홉(hop)별 의존성 latency (ms)</h3>'
            f'<table><thead><tr><th>source → target</th><th>p95</th><th>p99</th><th>fail %</th><th>n</th>'
            f'</tr></thead><tbody>{hrows or "<tr><td colspan=5 class=muted>데이터 없음</td></tr>"}</tbody></table></div>'
            '<p class="note">메트릭의 이상(예: throttling)과 이 trace 지연을 같은 컴포넌트에서 교차하면 '
            '근본 원인 단서가 됩니다. 느린 span 은 App Insights end-to-end transaction 으로 분해하세요.</p>')
    else:
        trace_html = f'<p class="muted">트레이싱 비활성: {_esc(tr.error or "—")}</p>'

    # 파드/이벤트/HPA
    if k.error:
        pod_html = f'<p class="err">Kubernetes 수집 실패: {_esc(k.error)}</p>'
    elif not k.pods:
        pod_html = '<p class="muted">파드 없음</p>'
    else:
        rows = ""
        for pod in sorted(k.pods, key=lambda x: (x.component, x.name)):
            # 현재 상태를 우선 표시한다. last_state.terminated.reason은 과거 종료 이력이므로
            # 현재 Ready인 Pod를 Error로 과장 표시하지 않는다.
            if pod.waiting_reason:
                state = pod.waiting_reason
            elif pod.phase == "Pending":
                state = "Pending"
            elif not pod.ready and pod.term_reason:
                state = pod.term_reason
            else:
                state = pod.phase
            cls = "bad" if (pod.waiting_reason or pod.phase == "Pending" or (not pod.ready and pod.term_reason)) else ""
            ns_prefix = f"{_esc(pod.namespace)}/" if getattr(pod, "namespace", "") and cfg.all_namespaces else ""
            rows += (f"<tr><td>{ns_prefix}{_esc(pod.name)}</td><td>{COMP_LABEL_KO.get(pod.component,pod.component)}</td>"
                     f"<td class='{cls}'>{_esc(state)}</td><td>{'✓' if pod.ready else '—'}</td>"
                     f"<td>{pod.restarts}</td><td>{'—' if pod.has_limits else '없음'}</td>"
                     f"<td>{_esc(pod.node)}</td></tr>")
        pod_html = (f'<table><thead><tr><th>pod</th><th>component</th><th>state</th><th>ready</th>'
                    f'<th>restarts</th><th>limits</th><th>node</th></tr></thead><tbody>{rows}</tbody></table>')

    ev_html = ('<table><thead><tr><th>reason</th><th>object</th><th>count</th><th>message</th></tr></thead>'
               '<tbody>' + "".join(f"<tr><td>{_esc(e.reason)}</td><td>{_esc(e.obj)}</td><td>{e.count}</td>"
               f"<td>{_esc(e.message)}</td></tr>" for e in k.events) + '</tbody></table>') \
        if k.events else '<p class="muted">경고 이벤트 없음</p>'

    hpa_html = ('<table><thead><tr><th>HPA</th><th>component</th><th>current</th><th>desired</th>'
                '<th>min–max</th></tr></thead><tbody>' +
                "".join(f"<tr><td>{_esc(h.name)}</td><td>{COMP_LABEL_KO.get(h.component,h.component)}</td>"
                        f"<td>{h.current}</td><td>{h.desired}</td><td>{h.minr}–{h.maxr}</td></tr>"
                        for h in k.hpas) + '</tbody></table>') \
        if k.hpas else '<p class="muted">HPA 없음</p>'

    # ── PVC 테이블 — 디스크 사용률 포함 ─────────────────────────────────
    def _fmt_bytes(b):
        if b is None or b == 0:
            return "—"
        for unit in ["Ki", "Mi", "Gi", "Ti"]:
            b /= 1024
            if b < 1024:
                return f"{b:.1f}{unit}"
        return f"{b:.1f}Ti"

    if k.pvcs:
        pvc_rows = ""
        for pvc in k.pvcs:
            status_cls = "bad" if pvc.status in ("Lost", "Pending") else ""
            key = f"{pvc.namespace}/{pvc.name}"
            usage = p.pvc_usage.get(key, {}) if p.enabled else {}
            used_pct = usage.get("used_pct")
            used_b = usage.get("used_bytes")
            cap_b = usage.get("cap_bytes")
            if used_pct is None:
                if not p.enabled:
                    pct_cell = '<span class="muted" title="Prometheus 미연결">측정불가<br><small>Prometheus 필요</small></span>'
                else:
                    pct_cell = '<span class="muted" title="kubelet_volume_stats 메트릭 없음">미수집<br><small>kubelet 메트릭 없음</small></span>'
            elif used_pct >= 85:
                pct_cell = f'<span style="color:var(--crit);font-weight:700">🔴 {used_pct}%</span>'
            elif used_pct >= 70:
                pct_cell = f'<span style="color:var(--warn);font-weight:600">🟡 {used_pct}%</span>'
            else:
                pct_cell = f'<span style="color:var(--ok)">🟢 {used_pct}%</span>'
            # 사용/전체: Prometheus 없으면 K8s API의 capacity 값만이라도 표시
            if used_b is not None and (cap_b or pvc.capacity):
                size_cell = f"{_fmt_bytes(used_b)} / {_esc(pvc.capacity) or _fmt_bytes(cap_b)}"
            elif pvc.capacity:
                size_cell = f'<span class="muted">— </span>/ {_esc(pvc.capacity)}'
            else:
                size_cell = '<span class="muted">— / —</span>'
            pvc_rows += (
                f"<tr><td>{_esc(pvc.namespace)}/{_esc(pvc.name)}</td>"
                f"<td class='{status_cls}'>{_esc(pvc.status)}</td>"
                f"<td>{pct_cell}</td>"
                f"<td>{size_cell}</td>"
                f"<td>{_esc(pvc.storage_class)}</td></tr>")
        pvc_html = (
            f'<table><thead><tr><th>PVC (namespace/name)</th><th>상태</th>'
            f'<th>디스크 사용률</th><th>사용/전체</th><th>StorageClass</th>'
            f'</tr></thead><tbody>{pvc_rows}</tbody></table>'
            '<p class="note">🔴 85% 이상 — 즉시 확장 필요 &nbsp; 🟡 70~85% — 모니터링 강화'
            ' &nbsp; <span style="color:var(--muted)">사용률 미수집 시 Prometheus에서 kubelet_volume_stats 메트릭 확인 필요</span></p>')
    else:
        pvc_html = '<p class="muted">PVC 없음</p>'

    # ── Node 테이블 — CPU·Mem 요청 점유율 포함 ───────────────────────────
    if k.nodes:
        node_rows = ""
        for n in k.nodes:
            ready_cls = "bad" if not n.ready else ""
            ready_txt = "Ready" if n.ready else "NotReady"
            nu = p.node_usage.get(n.name, {}) if p.enabled else {}
            cpu_pct = nu.get("cpu_req_pct")
            mem_pct = nu.get("mem_req_pct")
            def _pct_badge(v):
                if v is None:
                    return '<span class="muted">— (Prometheus 필요)</span>'
                if v >= 90:
                    return f'<span style="color:var(--crit);font-weight:700">🔴 {v}% 위험</span>'
                if v >= 70:
                    return f'<span style="color:var(--warn);font-weight:600">🟡 {v}% 주의</span>'
                return f'<span style="color:var(--ok);font-weight:600">🟢 {v}% 양호</span>'
            node_rows += (
                f"<tr><td>{_esc(n.name)}</td>"
                f"<td class='{ready_cls}'>{ready_txt}</td>"
                f"<td>{_esc(', '.join(n.conditions) or '—')}</td>"
                f"<td>{_esc(n.cpu_alloc)}</td>"
                f"<td>{_pct_badge(cpu_pct)}</td>"
                f"<td>{_esc(n.mem_alloc)}</td>"
                f"<td>{_pct_badge(mem_pct)}</td>"
                f"<td class='muted'>{_esc(', '.join(n.taints) or '—')}</td></tr>")
        node_html = (
            f'<table><thead><tr><th>노드</th><th>상태</th><th>비정상 조건</th>'
            f'<th>CPU allocatable</th><th>CPU 요청률</th>'
            f'<th>Mem allocatable</th><th>Mem 요청률</th><th>Taint</th>'
            f'</tr></thead><tbody>{node_rows}</tbody></table>'
            '<p class="note">'
            '🟢 70% 미만 — 양호 &nbsp; 🟡 70~90% — 여유 부족, 신규 파드 스케줄 실패 가능 &nbsp; 🔴 90% 이상 — 노드 추가 필요'
            '</p>')
    else:
        node_html = '<p class="muted">노드 정보 없음</p>'

    # ── PDB 테이블 — 권고 포함 ────────────────────────────────────────────
    if k.pdbs:
        pdb_rows = ""
        for pdb in k.pdbs:
            disr = pdb.disruptions_allowed
            disr_cls = "bad" if (disr is not None and disr == 0) else ""
            disr_val = disr if disr is not None else "—"
            # 권고 메시지 생성
            if disr is not None and disr == 0:
                advice = "⚠️ 중단 불가 — 노드 업그레이드 차단 위험. maxUnavailable: 1 또는 레플리카 증가 필요"
            elif pdb.min_available and pdb.min_available == str(pdb.desired_healthy):
                advice = "🟡 minAvailable = 전체 파드 수 → 드레인 차단 위험. minAvailable 을 N-1 로 조정 권장"
            else:
                advice = "✅ 정상"
            pdb_rows += (
                f"<tr><td>{_esc(pdb.name)}</td>"
                f"<td>{_esc(pdb.min_available or '—')}</td>"
                f"<td>{_esc(pdb.max_unavailable or '—')}</td>"
                f"<td class='{disr_cls}'>{disr_val}</td>"
                f"<td>{pdb.current_healthy}/{pdb.desired_healthy}</td>"
                f"<td>{advice}</td></tr>")
        pdb_html = (f'<table><thead><tr><th>PDB</th><th>minAvailable</th><th>maxUnavailable</th>'
                    f'<th>허용중단</th><th>healthy</th><th>권고</th>'
                    f'</tr></thead><tbody>{pdb_rows}</tbody></table>'
                    '<p class="note">허용중단=0 이면 노드 유지보수·업그레이드 시 파드 드레인이 영구 차단됩니다.</p>')
    else:
        pdb_html = '<p class="muted">PDB 없음 — 중요 파드에는 PDB 설정을 권장합니다 (노드 업그레이드 안전성).</p>'

    # ── Capacity Planning ─────────────────────────────────────────────────
    import math as _math

    # 노드 현황 요약 (Capacity Planning 판단 기준)
    total_nodes = len(k.nodes)
    ready_nodes = sum(1 for n in k.nodes if n.ready)
    # Cluster Autoscaler 활성 여부: CA가 관리하는 노드엔 cluster-autoscaler.kubernetes.io/* annotation 존재
    # 여기선 노드 taint 중 'aks.azure.com' 계열이 없는 경우 CA 미확인으로 처리
    has_ca_hint = any("cluster-autoscaler" in " ".join(n.taints) for n in k.nodes)

    # 노드별 CPU/Mem 점유율에서 포화 여부 판단
    node_cpu_saturated = any(
        v.get("cpu_req_pct", 0) >= 85 for v in p.node_usage.values()
    ) if p.node_usage else False
    node_mem_saturated = any(
        v.get("mem_req_pct", 0) >= 85 for v in p.node_usage.values()
    ) if p.node_usage else False

    # 노드 요약 HTML
    node_saturation_warn = ""
    if node_cpu_saturated:
        node_saturation_warn += '<span style="color:var(--crit);font-weight:700">🔴 CPU 요청률 85% 이상인 노드 존재 — 파드 스케줄 실패 위험</span><br>'
    if node_mem_saturated:
        node_saturation_warn += '<span style="color:var(--crit);font-weight:700">🔴 메모리 요청률 85% 이상인 노드 존재 — OOM 위험</span><br>'
    if not node_saturation_warn and p.node_usage:
        node_saturation_warn = '<span style="color:var(--ok);font-weight:600">🟢 모든 노드 CPU·메모리 요청률 정상 범위</span><br>'
    elif not node_saturation_warn:
        node_saturation_warn = '<span class="muted">노드 리소스 사용률: Prometheus 미연결로 측정 불가</span><br>'
    ca_status = '✅ Cluster Autoscaler 감지됨' if has_ca_hint else '⚠️ Cluster Autoscaler 미감지 — 수동 스케일 필요'
    node_summary_html = (
        f'<div class="cap-node-summary">'
        f'<b>🖥️ 노드 현황</b>: 전체 <b>{total_nodes}</b>개 / Ready <b>{ready_nodes}</b>개 &nbsp;|&nbsp; {ca_status}<br>'
        f'{node_saturation_warn}'
        f'</div>'
    )

    cap_rows = ""
    for c in [c for c in COMPONENTS if c in rollup and c != "기타"]:
        r = rollup[c]
        throttle = r.throttle_max
        mem = r.mem_max

        # 파드별 실제 CPU 합산 → 컴포넌트 합계 (Prometheus 있을 때)
        label_c = COMP_LABEL_KO.get(c, c)
        comp_pods = [
            (f"{pod.namespace}/{pod.name}" if getattr(pod, "namespace", "") else pod.name)
            for pod in k.pods if pod.component == c
        ]
        actual_cpu_cores = sum(p.pod_cpu_actual.get(pn, 0) for pn in comp_pods) if p.enabled else None
        actual_cpu_cores = round(actual_cpu_cores, 3) if actual_cpu_cores else None

        hpa_info = next((h for h in k.hpas if c in h.component), None)
        current_rep = hpa_info.current if hpa_info else r.pods
        max_rep = hpa_info.maxr if hpa_info else None

        def _pct_bar(val, warn=70, crit=90, empty_msg="해당 pod 메트릭 없음"):
            if val is None:
                return f'<span class="muted">데이터 없음<br><small>{empty_msg}</small></span>'
            cls = "crit" if val >= crit else ("warn" if val >= warn else "ok")
            return (f'<div class="cap-bar-wrap"><div class="cap-bar cap-bar-{cls}" style="width:{min(val,100):.0f}%"></div>'
                    f'<span class="cap-bar-lbl">{val:.0f}%</span></div>')

        def _proj(val, mult):
            if val is None:
                return '<span class="muted">—</span>'
            projected = val * mult
            cls = "bad" if projected >= 90 else ("warn-text" if projected >= 70 else "")
            return f'<span class="{cls}">{projected:.0f}%</span>'

        def _rep_needed(cur_rep, cur_util, target_util=70):
            try:
                if cur_util is None or cur_rep is None or cur_util == 0:
                    return '<span class="muted">—</span>'
                if _math.isnan(cur_util) or _math.isinf(cur_util):
                    return '<span class="muted">—</span>'
                needed = cur_rep * cur_util / target_util
                n = max(int(needed) + 1, cur_rep)
                cls = "bad" if n > cur_rep else ""
                # 권장 레플리카가 노드 포화 상태에서 늘어날 경우 경고
                node_warn = " ⚠️노드추가필요" if (cls == "bad" and node_cpu_saturated) else ""
                return f'<span class="{cls}">{n}{node_warn}</span>'
            except Exception:
                return '<span class="muted">—</span>'

        cpu_extra = f'<br><small style="color:var(--muted)">실제사용 {actual_cpu_cores} core</small>' if actual_cpu_cores else ""
        if throttle is None and r.no_limit_pods == r.pods and r.pods > 0:
            throttle_now = '<span class="muted">해당 없음<br><small>CPU limit 없음</small></span>'
            throttle_p20 = '<span class="muted">—</span>'
            throttle_p50 = '<span class="muted">—</span>'
            throttle_rep = '<span class="muted">—</span>'
        else:
            throttle_now = _pct_bar(throttle, 25, 50)
            throttle_p20 = _proj(throttle, 1.2)
            throttle_p50 = _proj(throttle, 1.5)
            throttle_rep = _rep_needed(current_rep, throttle)
        cpu_row = f"""
        <tr>
          <td><b>{_esc(label_c)}</b></td>
          <td>CPU throttle{cpu_extra}</td>
          <td>{throttle_now}</td>
          <td>{throttle_p20}</td>
          <td>{throttle_p50}</td>
          <td>{throttle_rep}</td>
          <td>{current_rep} / {max_rep or '—'}</td>
        </tr>"""
        if mem is not None:
            mem_now = _pct_bar(mem, 70, 90)
            mem_p20 = _proj(mem, 1.2)
            mem_p50 = _proj(mem, 1.5)
        elif r.mem_bytes_max is not None:
            mem_now = f'<span>{_fmt_bytes(r.mem_bytes_max)}</span><br><small class="muted">limit 없음</small>'
            mem_p20 = f'<span>{_fmt_bytes(r.mem_bytes_max * 1.2)}</span>'
            mem_p50 = f'<span>{_fmt_bytes(r.mem_bytes_max * 1.5)}</span>'
        else:
            mem_now = _pct_bar(None, 70, 90)
            mem_p20 = '<span class="muted">—</span>'
            mem_p50 = '<span class="muted">—</span>'

        mem_row = f"""
        <tr>
          <td></td>
          <td>Memory</td>
          <td>{mem_now}</td>
          <td>{mem_p20}</td>
          <td>{mem_p50}</td>
          <td>—</td>
          <td>—</td>
        </tr>"""
        cap_rows += cpu_row + mem_row

    if cap_rows:
        prom_note = "" if p.enabled else '<p class="note" style="color:var(--warn)">⚠️ Prometheus 미연결 — throttle/메모리 실측값 없음. <code>--prometheus-url</code> 옵션을 추가하면 실제 수치로 채워집니다.</p>'
        cap_html = f"""
        {node_summary_html}
        {prom_note}
        <div class="cap-note">
          📌 <b>읽는 법</b>: 현재 CPU throttle / 메모리 사용률 기반으로 +20%·+50% 부하 시 예상 사용률을 선형 추정합니다.
          <b>권장 레플리카</b>는 throttle 70% 이하 유지에 필요한 최소 파드 수입니다.
          ⚠️노드추가필요 표시는 현재 노드가 포화 상태여서 파드만 늘려서는 해결되지 않음을 의미합니다.
        </div>
        <table>
          <thead><tr>
            <th>컴포넌트</th><th>지표</th><th>현재 사용률</th>
            <th>+20% 부하 시</th><th>+50% 부하 시</th>
            <th>권장 레플리카</th><th>현재/최대 레플리카</th>
          </tr></thead>
          <tbody>{cap_rows}</tbody>
        </table>
        <div class="cap-guide">
          <h4>📋 부하 증가 대응 체크리스트</h4>
          <div class="cap-checks">
            <div class="cap-check"><span class="cap-check-icon">🔴</span>
              <div><b>+20% 대응 (단기) — 파드 수평 확장</b><br>
              HPA maxReplicas 여유 확인 · CPU limit 상향<br>
              <b>단, 노드 포화 시 파드 확장 전 노드 추가 필요</b><br>
              <code>kubectl edit hpa &lt;name&gt; -n &lt;ns&gt;</code></div></div>
            <div class="cap-check"><span class="cap-check-icon">🟠</span>
              <div><b>+50% 대응 (중기) — 노드 확장</b><br>
              수동: <code>az aks scale -g &lt;RG&gt; -n &lt;AKS&gt; --node-count &lt;N&gt;</code><br>
              자동(CA): <code>az aks update --enable-cluster-autoscaler --min-count 1 --max-count 10</code><br>
              CPU limit 제거 + HPA targetCPU 60% 설정 권장</div></div>
            <div class="cap-check"><span class="cap-check-icon">🟡</span>
              <div><b>장기 튜닝</b><br>
              VPA(Vertical Pod Autoscaler) 추천값 적용<br>
              Karpenter/KEDA 도입 · 비용 최적화(spot 노드 혼용)</div></div>
          </div>
        </div>"""
    else:
        cap_html = '<p class="muted">Capacity 데이터 없음 (파드 수집 필요)</p>'

    thr_s = p.series.get("throttle")
    if thr_s and not thr_s.error and thr_s.values:
        avg_v = thr_s.avg
        if avg_v is None:
            thr_status = ""
            thr_status_cls = "ok"
        elif avg_v >= 50:
            thr_status = '🔴 위험 — CPU limit에 심각하게 걸림'
            thr_status_cls = "crit"
        elif avg_v >= 25:
            thr_status = '🟡 주의 — CPU limit 상향 검토 필요'
            thr_status_cls = "warn"
        else:
            thr_status = '🟢 양호 — throttle 낮음'
            thr_status_cls = "ok"
        spark_html = f"""
<div class="metric-card">
  <div class="metric-header">
    <div class="metric-name">
      <span class="dot" style="background:#c8362f"></span>
      클러스터 CPU Throttle 추이
    </div>
    <div class="metric-badge metric-badge-{thr_status_cls}">{thr_status}</div>
  </div>
  <div class="metric-desc">
    전체 Pod의 <b>가중 평균</b> throttle 비율 (throttled_periods / total_periods).
    특정 Pod가 CPU limit에 집중적으로 걸릴수록 이 수치가 올라갑니다.
    각 컴포넌트별 throttle은 위 Capacity Planning 표를 참고하세요.
  </div>
  <div class="metric-vals">
    <span>평균 <b>{thr_s.avg}%</b></span>
    <span>최대 <b>{thr_s.mx}%</b></span>
  </div>
  <div class="metric-graph">{_spark(thr_s.values, "#c8362f")}</div>
</div>"""
    elif p.enabled:
        spark_html = '<p class="muted">throttle 시계열 없음</p>'
    else:
        spark_html = f'<p class="muted">Prometheus 비활성: {_esc(p.error or "—")}</p>'

    if baseline:
        prev_health = baseline.get("health_score")
        prev_risk   = (100 - prev_health) if prev_health is not None else None
        delta = risk - (prev_risk or 0)
        # 위험도가 올라가면(delta>0) 나쁜 방향 → 빨간색
        dcls = SEV_CRIT if delta > 0 else SEV_OK
        base_html = (f'<p class="kv">Baseline <b>{_esc(baseline.get("generated_at"))}</b> 대비 · '
                     f'위험도 <b>{prev_risk} → {risk}</b> '
                     f'(<span style="color:var(--{dcls})">{"+" if delta>=0 else ""}{delta}</span>)</p>')
    else:
        base_html = '<p class="muted">이전 스냅샷 없음 (다음 실행부터 추세 비교).</p>'

    ns_label = "전체 네임스페이스 (-A)" if cfg.all_namespaces else f"namespace: <b>{_esc(cfg.namespace)}</b>"

    return f"""<!DOCTYPE html><html lang="ko"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Azure Kubernetes Service 구조/성능 진단</title>
<style>
 :root{{--ink:#15202b;--bg:#eef1f5;--card:#fff;--text:#1f2933;--muted:#64748b;--border:#e2e8f0;
  --azure:#0078D4;--crit:#c8362f;--warn:#b07b00;--info:#0a6cbd;--ok:#107c10;}}
 *{{box-sizing:border-box}}
 body{{margin:0;background:var(--bg);color:var(--text);line-height:1.5;
  font-family:"Segoe UI",system-ui,-apple-system,"Malgun Gothic",sans-serif}}
 code,table,.mono{{font-family:"Cascadia Code","Consolas",ui-monospace,monospace}}
 .wrap{{max-width:1080px;margin:0 auto;padding:0 20px 64px}}
 header{{background:var(--ink);color:#e8eef4;padding:28px 0}}
 .hd{{max-width:1080px;margin:0 auto;padding:0 20px;display:flex;justify-content:space-between;
  align-items:center;gap:24px;flex-wrap:wrap}}
 header h1{{font-size:20px;margin:0 0 6px;font-weight:650}}
 .lvl{{display:inline-block;font-size:10.5px;letter-spacing:1px;background:rgba(0,120,212,.25);
  border:1px solid rgba(120,180,230,.4);color:#cfe6fb;border-radius:20px;padding:1px 9px;margin-left:8px}}
 header .meta{{font-size:12.5px;color:#9fb2c4}} header .meta b{{color:#cfe0ee}}
 .score{{text-align:center;padding:10px 22px;border-radius:12px;background:rgba(255,255,255,.06);
  border:1px solid rgba(255,255,255,.12)}}
 .score .num{{font-size:38px;font-weight:700;line-height:1}}
 .score .lab{{font-size:11px;letter-spacing:2px;text-transform:uppercase;margin-top:4px}}
 .score.ok .num{{color:#5dd55d}}.score.info .num{{color:#4db8ff}}.score.warning .num{{color:#ffcf4d}}.score.critical .num{{color:#ff7a72}}
 .summary{{display:flex;gap:12px;margin:22px 0 8px;flex-wrap:wrap}}
 .stat{{flex:1;min-width:120px;background:var(--card);border:1px solid var(--border);border-radius:10px;padding:14px 16px}}
 .stat .n{{font-size:26px;font-weight:700}}.stat .l{{font-size:12px;color:var(--muted)}}
 .stat.crit .n{{color:var(--crit)}}.stat.warn .n{{color:var(--warn)}}.stat.info .n{{color:var(--info)}}
 h2{{font-size:15px;text-transform:uppercase;letter-spacing:1.5px;color:var(--muted);
  margin:34px 0 14px;padding-bottom:8px;border-bottom:1px solid var(--border)}}
 .comps{{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:12px}}
 .comp-card{{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:14px 16px}}
 .comp-name{{font-size:15px;font-weight:700;margin-bottom:10px}}
 .comp-grid{{display:grid;grid-template-columns:1fr 1fr;gap:8px}}
 .comp-grid div{{font-size:12px;color:var(--muted);display:flex;justify-content:space-between;
  border-bottom:1px dashed var(--border);padding-bottom:4px}}
 .comp-grid div.wide{{grid-column:1/3}} .comp-grid b{{color:var(--text);font-size:14px}}
 .finding{{background:var(--card);border:1px solid var(--border);border-left:4px solid var(--muted);
  border-radius:10px;padding:16px 18px;margin-bottom:12px}}
 .finding.sev-critical{{border-left-color:var(--crit)}}.finding.sev-warning{{border-left-color:var(--warn)}}
 .finding.sev-info{{border-left-color:var(--info)}}.finding.sev-ok{{border-left-color:var(--ok)}}
 .finding-head{{display:flex;align-items:center;gap:10px;flex-wrap:wrap}}
 .finding-head h3{{font-size:15px;margin:0;flex-basis:100%}}
 .chip{{font-size:11px;font-weight:700;padding:2px 9px;border-radius:20px;color:#fff}}
 .chip-critical{{background:var(--crit)}}.chip-warning{{background:var(--warn)}}
 .chip-info{{background:var(--info)}}.chip-ok{{background:var(--ok)}}
 .cat{{font-size:11.5px;color:var(--muted);border:1px solid var(--border);padding:1px 8px;border-radius:20px}}
 .detail{{margin:10px 0;font-size:14px}}
 .reco{{background:#f1f6fb;border:1px solid #d8e7f4;border-radius:8px;padding:10px 12px;font-size:13.5px}}
 .reco-label{{display:inline-block;font-size:11px;font-weight:700;color:var(--azure);margin-right:8px;
  text-transform:uppercase;letter-spacing:.5px}}
 .panel{{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:16px 18px}}
 .kv{{font-size:14px;margin:0}}.kv b{{color:var(--azure)}}
 .block{{margin-bottom:18px}} .block h3{{font-size:14px;margin:0 0 8px;color:#8661c5}}
 .metric-card{{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:20px 22px;max-width:680px;width:100%}}
 .metric-header{{display:flex;justify-content:space-between;align-items:flex-start;gap:8px;margin-bottom:8px}}
 .metric-name{{font-size:13px;font-weight:600;display:flex;align-items:center;gap:7px}}
 .dot{{width:9px;height:9px;border-radius:50%;display:inline-block}}
 .metric-desc{{font-size:12px;color:var(--muted);line-height:1.5;margin-bottom:8px;padding:6px 8px;background:#f8fbff;border-radius:6px;border-left:3px solid var(--border)}}
 .metric-badge{{font-size:11.5px;font-weight:600;padding:2px 8px;border-radius:12px;white-space:nowrap}}
 .metric-badge-ok{{background:#e6f4ea;color:#107c10}}
 .metric-badge-warn{{background:#fff8e1;color:#b07b00}}
 .metric-badge-crit{{background:#fde8e8;color:#c8362f}}
 .metric-vals{{display:flex;justify-content:space-between;font-size:13px;color:var(--muted);margin:6px 0 8px}}
 .metric-vals b{{color:var(--text);font-size:17px}}
 .metric-graph{{margin-top:6px}} svg.spark{{width:100%;height:60px;display:block}}
 table{{width:100%;border-collapse:collapse;font-size:12px;background:var(--card);
  border:1px solid var(--border);border-radius:8px;overflow:hidden}}
 th{{background:#f3f6f9;text-align:left;padding:7px 9px;font-weight:600;color:#42526b;
  border-bottom:1px solid var(--border);white-space:nowrap}}
 td{{padding:6px 9px;border-bottom:1px solid #eef2f6;vertical-align:top;overflow-wrap:anywhere}}
 td.bad{{color:var(--crit);font-weight:600}} tbody tr:hover{{background:#f8fbff}}
 .muted{{color:var(--muted);font-size:13px}}.err{{color:var(--crit);font-size:12.5px}}
 .note{{color:var(--muted);font-size:12px;margin:4px 0 8px}}
 .steps-btn{{margin-top:10px;background:none;border:1px solid var(--azure);color:var(--azure);
  border-radius:6px;padding:5px 12px;font-size:12.5px;cursor:pointer;transition:all .15s}}
 .steps-btn:hover{{background:var(--azure);color:#fff}}
 .steps-list{{margin:10px 0 0 0;padding:0 0 0 20px;font-size:12.5px;
  background:#f6f9fc;border:1px solid #d8e7f4;border-radius:6px;padding:10px 10px 10px 28px}}
 .steps-list li{{margin-bottom:6px;font-family:"Cascadia Code","Consolas",monospace;
  color:#1a3a5c;line-height:1.6}}
 .steps-list li:last-child{{margin-bottom:0}}
 .warn-text{{color:var(--warn);font-weight:600}}
 .cap-note{{font-size:12.5px;color:var(--muted);background:#f8fbff;border:1px solid #d8e7f4;
  border-radius:8px;padding:10px 14px;margin-bottom:12px}}
 .cap-bar-wrap{{display:flex;align-items:center;gap:8px;min-width:140px}}
 .cap-bar{{height:10px;border-radius:4px;transition:width .3s}}
 .cap-bar-ok{{background:var(--ok)}}.cap-bar-warn{{background:var(--warn)}}.cap-bar-crit{{background:var(--crit)}}
 .cap-bar-lbl{{font-size:12px;white-space:nowrap;color:var(--text)}}
 .cap-guide{{margin-top:18px;background:var(--card);border:1px solid var(--border);
  border-radius:10px;padding:16px 18px}}
 .cap-guide h4{{margin:0 0 12px;font-size:14px;color:#42526b}}
 .cap-checks{{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:12px}}
 .cap-check{{display:flex;gap:12px;align-items:flex-start;font-size:13px;
  background:#f8fbff;border:1px solid var(--border);border-radius:8px;padding:12px}}
 .cap-check-icon{{font-size:20px;flex-shrink:0}}
 .cap-node-summary{{background:#f0f4f8;border:1px solid #c8d8e8;border-radius:8px;padding:10px 14px;margin-bottom:12px;font-size:13px;line-height:1.7}}
 footer{{color:var(--muted);font-size:12px;margin-top:40px;padding-top:16px;border-top:1px solid var(--border)}}
 @media (max-width:640px){{.hd{{flex-direction:column;align-items:flex-start}}}}
</style>
<script>
function toggleSteps(id) {{
  var el = document.getElementById(id);
  var btn = el.previousElementSibling;
  if (el.style.display === 'none') {{
    el.style.display = 'block';
    btn.textContent = '▼ 조치 단계 닫기';
  }} else {{
    el.style.display = 'none';
    btn.textContent = '▶ 상세 조치 단계 보기';
  }}
}}
</script>
</head><body>
<header><div class="hd">
  <div><h1>Azure Kubernetes Service 구조/성능 진단<span class="lvl">K8s + PROM + TRACE</span></h1>
    <div class="meta">{ns_label}
      · 생성 {_esc(generated_at)}</div></div>
  <div class="score {risk_cls}"><div class="num">{risk}</div><div class="lab">{risk_label}</div></div>
</div></header>
<div class="wrap">
  <div class="summary">
    <div class="stat crit"><div class="n">{nc}</div><div class="l">위험 (Critical)</div></div>
    <div class="stat warn"><div class="n">{nw}</div><div class="l">주의 (Warning)</div></div>
    <div class="stat info"><div class="n">{ni}</div><div class="l">정보 (Info)</div></div>
  </div>

  <h2>컴포넌트 요약</h2>
  <div class="comps">{comp_cards}</div>

  <h2>발견사항 및 권장 조치</h2>
  {finding_cards}

  <h2>컴포넌트 간 흐름 · 트레이싱 (OTel / App Insights)</h2>
  {trace_html}

  <h2>Prometheus 신호</h2>
  {spark_html}

  <h2>파드 상태 (Kubernetes API)</h2>
  {pod_html}

  <h2>경고 이벤트</h2>
  {ev_html}

  <h2>HPA</h2>
  {hpa_html}

  <h2>🗄️ 스토리지 (PVC)</h2>
  {pvc_html}

  <h2>🛡️ 가용성 (PDB)</h2>
  {pdb_html}

  <h2>🖥️ 노드 상태</h2>
  {node_html}

  <h2>📈 Capacity Planning — 부하 증가 시뮬레이션</h2>
  {cap_html}

  <footer>
    <p>Kubernetes API + Prometheus + App Insights(OTel). Azure 리소스 쿼리 미사용.
    읽기 전용(get/list, PromQL query, KQL query, Blob read/write)이며 클러스터를 변경하지 않습니다.</p>
    <p>상관: 같은 컴포넌트에서 trace 지연과 메트릭 이상(throttling 등)이 겹치면 근본 원인 단서.
    임계치는 일반 휴리스틱이며 특화 쿼리(lag/p99/GPU, trace 테이블 모드)는 환경에 맞게 조정하세요.</p>
  </footer>
</div></body></html>"""


# ──────────────────────────────────────────────────────────────────────────
# 데모
# ──────────────────────────────────────────────────────────────────────────
def demo_k8s() -> K8sData:
    d = K8sData()
    d.pods = [
        PodInfo("aggregator-7c9d-aa", "aggregator", "Running", True, 0, node="aks-pool1-1"),
        PodInfo("aggregator-7c9d-bb", "aggregator", "Running", True, 3, node="aks-pool1-2"),
        PodInfo("detector-5f8b-aa", "detector", "Running", True, 1, node="aks-pool1-1"),
        PodInfo("detector-5f8b-bb", "detector", "Running", False, 7, term_reason="OOMKilled", node="aks-pool1-3"),
        PodInfo("trainer-job-9xk", "trainer", "Pending", False, 0, has_limits=False, node=""),
        PodInfo("trainer-job-prev", "trainer", "Running", True, 0, has_limits=False, node="aks-gpu-1"),
    ]
    d.events = [
        EventInfo("FailedScheduling", "Pod/trainer-job-9xk", "0/6 nodes available: 6 Insufficient nvidia.com/gpu", 12),
        EventInfo("BackOff", "Pod/detector-5f8b-bb", "Back-off restarting failed container", 9),
    ]
    d.hpas = [HPAInfo("detector-hpa", "detector", 8, 8, 2, 8),
              HPAInfo("aggregator-hpa", "aggregator", 3, 3, 2, 10)]
    return d


def demo_prom() -> PromData:
    import math
    p = PromData(enabled=True)
    p.per_pod = {"throttle_pct": {"detector-5f8b-aa": 46.0, "detector-5f8b-bb": 52.0,
                                  "aggregator-7c9d-aa": 8.0},
                 "mem_pct": {"detector-5f8b-bb": 97.0, "detector-5f8b-aa": 71.0},
                 "restarts": {"detector-5f8b-bb": 7}}
    p.specialty = {"lag": 84000.0, "p99": 0.85, "gpu": 28.0}
    vals = [round(20 + 25 * abs(math.sin(i / 4.0)) + (i % 3) * 2, 1) for i in range(60)]
    vals[40] = 58.0
    p.series["throttle"] = MetricSeries("throttle", "%", vals)
    return p


def demo_trace() -> TraceData:
    d = TraceData(enabled=True)
    d.comp = {
        "aggregator": CompLatency("aggregator", 42.0, 61.0, 0.3, 184220),
        "detector": CompLatency("detector", 410.0, 850.0, 2.1, 152119),
        "trainer": CompLatency("trainer", 120.0, 240.0, 0.0, 902),
    }
    d.hops = [
        HopStat("aggregator", "detector", 380.0, 820.0, 2.0, 152119),
        HopStat("detector", "feature-store", 60.0, 140.0, 0.1, 152119),
        HopStat("aggregator", "eventhub", 12.0, 28.0, 0.0, 184220),
    ]
    return d


def demo_baseline() -> dict:
    return {"schema_version": 1, "namespace": "ml-pipeline", "generated_at": "2026-06-02 09:00:00",
            "health_score": 80, "components": {"aggregator": {"restarts": 0, "throttle_max": 9.0},
            "detector": {"restarts": 1, "throttle_max": 20.0}, "trainer": {"restarts": 0, "throttle_max": None}}}


# ──────────────────────────────────────────────────────────────────────────
def parse_args(argv) -> Config:
    ba = argparse.BooleanOptionalAction
    ap = argparse.ArgumentParser(description="AKS 처리 구조 진단 → HTML (K8s+Prometheus+Tracing, Blob 적재)")
    ap.add_argument("--namespace", "-n", default="default")
    ap.add_argument("--all-namespaces", "-A", action="store_true", help="전체 네임스페이스 수집")
    ap.add_argument("--context"); ap.add_argument("--in-cluster", action="store_true")
    ap.add_argument("--component-label", default="component")
    ap.add_argument("--k8s", action=ba, default=True)
    ap.add_argument("--prometheus-url"); ap.add_argument("--prometheus-aad", action="store_true")
    ap.add_argument("--hours", type=int, default=1); ap.add_argument("--step-min", type=int, default=1)
    ap.add_argument("--appinsights-id", help="App Insights 리소스 ID 또는 LA 워크스페이스 ID (트레이싱)")
    ap.add_argument("--trace-hours", type=int, default=1)
    ap.add_argument("--trace-table-mode", choices=["classic", "workspace"], default="classic")
    ap.add_argument("--blob-account-url", help="https://<acct>.blob.core.windows.net (리포트·이력 적재)")
    ap.add_argument("--blob-container", default="aks-diagnose")
    ap.add_argument("--history", action=ba, default=True)
    ap.add_argument("--history-dir", default="./aks_diagnose_history")
    ap.add_argument("--out", default="aks_report.html")
    ap.add_argument("--demo", action="store_true")
    a = ap.parse_args(argv)
    return Config(namespace=a.namespace, all_namespaces=a.all_namespaces,
                  context=a.context, in_cluster=a.in_cluster,
                  component_label=a.component_label, k8s=a.k8s, prometheus_url=a.prometheus_url,
                  prometheus_aad=a.prometheus_aad, hours=a.hours, step_min=a.step_min,
                  appinsights_id=a.appinsights_id, trace_hours=a.trace_hours,
                  trace_table_mode=a.trace_table_mode, blob_account_url=a.blob_account_url,
                  blob_container=a.blob_container, history=a.history, history_dir=a.history_dir,
                  out=a.out, demo=a.demo)


def main(argv=None) -> int:
    cfg = parse_args(argv if argv is not None else sys.argv[1:])

    # --out - 모드: stderr를 /dev/null 로 리다이렉트해서 kubectl logs가 stdout만 받도록 함
    if cfg.out == "-" and not cfg.demo:
        import os as _os
        devnull = open(_os.devnull, "w")
        sys.stderr = devnull
    now = dt.datetime.now()
    generated_at = now.strftime("%Y-%m-%d %H:%M")
    ts = now.strftime("%Y%m%d_%H%M%S")

    store = None
    if cfg.demo:
        k, p, tr, baseline = demo_k8s(), demo_prom(), demo_trace(), demo_baseline()
    else:
        if cfg.blob_account_url:
            try:
                store = BlobStore(cfg)
            except Exception as e:  # noqa: BLE001
                print(f"참고: Blob 초기화 실패 — {e}", file=sys.stderr)
        k = K8sCollector(cfg).collect() if cfg.k8s else K8sData(error="K8s 수집 비활성(--no-k8s)")
        p = PrometheusCollector(cfg).collect()
        tr = TraceCollector(cfg).collect()
        baseline = load_baseline(cfg, store)
        if cfg.k8s and k.error:
            print(f"참고: Kubernetes 수집 실패 — {k.error}", file=sys.stderr)
        if not cfg.prometheus_url:
            print("참고: --prometheus-url 미지정 → Prometheus 생략.", file=sys.stderr)
        if not cfg.appinsights_id:
            print("참고: --appinsights-id 미지정 → 트레이싱 생략.", file=sys.stderr)

    az = Analyzer(k, p, tr, cfg)
    findings = az.run()
    score = health_score(findings)
    save_snapshot(cfg, score, az.rollup, store)

    out_html = render_html(cfg, k, p, tr, az.rollup, findings, baseline, generated_at)

    nc = sum(1 for f in findings if f.severity == SEV_CRIT)
    with open(cfg.out, "w", encoding="utf-8") as fh:
        fh.write(out_html)
    if store:
        try:
            name = store.upload_report(cfg.out, cfg.namespace, ts)
            print(f"Blob 업로드: {cfg.blob_container}/{name}")
        except Exception as e:  # noqa: BLE001
            print(f"참고: Blob 업로드 실패 — {e}", file=sys.stderr)
    risk = 100 - score
    print(f"리포트 생성 완료: {cfg.out}  (위험도: {risk}/100, 위험 {nc}건)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
