# AKS Diagnose Tool

AKS 클러스터의 **구조·성능·안정성을 자동 진단**하고 HTML 리포트를 생성하는 도구입니다.

- Kubernetes API, Prometheus, App Insights 3개 계층에서 데이터 수집
- Pod 상태 / 재시작 / OOMKill / Pending / CPU throttle / 메모리 / HPA / PVC / PDB / 노드 분석
- 발견사항을 위험(Critical) · 주의(Warning) · 정보(Info) 로 분류하고 조치 단계 제시
- kubectl 만 있으면 실행 가능 (Python / Docker 로컬 설치 불필요)

---

## 소스 파일 구성

| 파일 | 설명 |
|------|------|
| `aks_diagnose.py` | 핵심 진단 엔진. K8s API / Prometheus / App Insights 수집 → HTML 리포트 생성 |
| `Dockerfile` | `python:3.12-slim` 기반 컨테이너 이미지 |
| `requirements.txt` | Python 의존성 (kubernetes, requests, azure-identity 등) |
| `build-image.ps1` | ACR 클라우드 빌드 + push (Windows) |
| `build-image.sh` | ACR 클라우드 빌드 + push (Linux / macOS) |
| `run-diagnose.ps1` | K8s Pod 실행 → HTML 리포트 로컬 복사 (Windows) |
| `run-diagnose.sh` | K8s Pod 실행 → HTML 리포트 로컬 복사 (Linux / macOS) |
| `k8s/rbac.yaml` | 진단 Pod 에 필요한 ServiceAccount + ClusterRole (읽기 전용) |
| `aks-rbac.yaml` | 운영(CronJob) 용 RBAC — Workload Identity 포함 |
| `aks-cronjob.yaml` | 정기 실행 CronJob 매니페스트 (운영용) |

> Windows 환경에서는 `.ps1`, Linux / macOS 환경에서는 `.sh` 스크립트를 사용합니다.

---

## 사전 준비사항

진단 도구를 실행하기 전에 아래 조건이 모두 충족되어야 합니다.

### 1. kubectl 설치 및 클러스터 접근 가능

로컬 머신에 `kubectl` 이 설치되어 있고, 대상 클러스터에 접근 가능한 상태여야 합니다.

```powershell
# kubectl 설치 확인
kubectl version --client

# 현재 연결된 클러스터 확인 — 올바른 클러스터가 나오면 준비 완료
kubectl config current-context
```

> 위 명령으로 대상 클러스터가 이미 표시된다면 추가 설정은 불필요합니다.

**AKS 클러스터에 아직 연결되지 않은 경우에만** 아래 명령으로 kubeconfig 를 설정하세요.

```powershell
# AKS 전용 — kubeconfig 에 클러스터 정보가 없을 때만 실행
az aks get-credentials --resource-group <RG> --name <AKS_NAME>
```

> `az aks get-credentials` 는 Azure AKS 전용 명령입니다.
> 온프레미스 · GKE · EKS 등 비AKS 환경에서는 해당 Kubernetes 배포판의 kubeconfig 설정 방법을 따르세요.

### 2. kubectl 권한 요건

`run-diagnose.ps1` 실행 시 **`k8s/rbac.yaml` 을 자동으로 apply** 합니다.
이 작업에는 아래 권한이 필요합니다.

| 필요 권한 | 이유 |
|-----------|------|
| `ServiceAccount` 생성 | 진단 Pod 용 SA(`aks-diagnose`) 생성 |
| `ClusterRole` / `ClusterRoleBinding` 생성 | 클러스터 전체 읽기 권한 부여 |
| `Pod` 생성 / 삭제 | 진단 Pod 실행 및 정리 |

> **확인 방법**: 아래 명령으로 현재 계정이 ClusterRole 을 생성할 수 있는지 확인하세요.
> ```powershell
> kubectl auth can-i create clusterroles
> kubectl auth can-i create clusterrolebindings
> ```
> 결과가 `yes` 여야 합니다. `no` 인 경우 클러스터 관리자에게 권한을 요청하세요.

### 3. 진단 Pod 의 ServiceAccount 권한 (`k8s/rbac.yaml`)

`run-diagnose.ps1` 이 자동으로 적용하는 `k8s/rbac.yaml` 은
진단 Pod(`aks-diagnose`) 에 아래 **읽기 전용(Read-Only)** 권한을 부여합니다.

```
pods, nodes, events, persistentvolumeclaims, namespaces, services
deployments, replicasets, statefulsets, daemonsets
horizontalpodautoscalers
poddisruptionbudgets
jobs, cronjobs
```

> 이 ClusterRole 은 **읽기(`get`, `list`)만 허용**하며 클러스터 리소스를 변경하지 않습니다.

### 4. (선택) Prometheus 접근 가능

Prometheus 가 클러스터 내에 설치되어 있으면 CPU throttle · 메모리 사용률 등 상세 지표를 수집합니다.
없어도 Kubernetes API 기반 진단은 정상 동작합니다.

---

## 실행 방식
아래 2가지 방법 중 한가지 방법으로 실행 가능합니다.

### 방법 1. Kubernetes Pod 로 직접 실행 (권장)

로컬에 Python / Docker 설치 없이 **kubectl 만으로** 진단을 실행합니다.
진단 도구가 클러스터 내부에서 실행되므로 K8s API 접근이 가장 안정적입니다.

#### 1단계 — 이미지 빌드 (인터넷 접근가능 환경인 경우 Skip가능, 최초 1회 또는 소스 변경 시)
현재 azure repository에 (factorykr)에 최신이미지가 upload되어 있으므로 Kubernetes에서 Azure Repo로의 접근이 가능한 환경이면 이 단계는 Skip가능합니다.

```powershell
# Windows
.\build-image.ps1
```

```bash
# Linux / macOS
./build-image.sh
```

#### 2단계 — 진단 실행


```powershell
# Windows — 전체 네임스페이스 진단
.\run-diagnose.ps1

# 특정 네임스페이스만
.\run-diagnose.ps1 -Namespace ml-pipeline

# Prometheus URL 지정
.\run-diagnose.ps1 -PrometheusUrl http://my-prometheus:9090

# 출력 파일 경로 지정
.\run-diagnose.ps1 -Out C:\reports\report.html
```

```bash
# Linux / macOS
./run-diagnose.sh
./run-diagnose.sh -n ml-pipeline
./run-diagnose.sh -p http://my-prometheus:9090
./run-diagnose.sh -o /tmp/report.html
```


> **이미지 출처**: `run-diagnose.ps1` (및 `run-diagnose.sh`) 는 기본적으로
> **Azure Container Registry `factorykr.azurecr.io/aks-diagnose:latest`** 에서 이미지를 다운로드해 실행합니다.
> 해당 ACR 은 anonymous pull(인증 없이 다운로드) 이 허용된 public 저장소입니다.
>
> 이미지 경로를 바꾸려면 스크립트의 `-Image` 옵션을 사용하거나,
> `run-diagnose.ps1` 상단의 `$Image` 기본값을 직접 수정하세요.
> ```powershell
> # 다른 레지스트리 이미지 사용 예
> .\run-diagnose.ps1 -Image myregistry.azurecr.io/aks-diagnose:v2
> ```

> **Prometheus 기본 URL**: `run-diagnose.ps1` (및 `run-diagnose.sh`) 는 Prometheus 주소로
> `http://prom-prometheus-server.monitoring.svc.cluster.local:80` 을 기본값으로 사용합니다.
> 이는 `monitoring` 네임스페이스에 Helm Chart 이름 `prom` 으로 설치된 kube-prometheus-stack 기준입니다.
>
> 환경이 다른 경우 `-PrometheusUrl` 옵션으로 직접 지정하세요.
> ```powershell
> # Prometheus URL 이 다른 경우
> .\run-diagnose.ps1 -PrometheusUrl http://prometheus.monitoring.svc.cluster.local:9090
> ```
> ```bash
> ./run-diagnose.sh -p http://prometheus.monitoring.svc.cluster.local:9090
> ```

---

#### (선택) Azure Managed Prometheus 자동 연결 — `-UseAzureManagedPrometheus` / `--use-azure-managed-prometheus`

AKS 생성 시 **Azure Monitor 관리형 Prometheus** 를 활성화한 환경이라면
이 옵션 하나만으로 Prometheus 주소를 자동으로 감지·연결합니다.
별도로 URL 을 찾거나 입력할 필요가 없습니다.

```powershell
# Windows — Azure Managed Prometheus 자동 연결
.\run-diagnose.ps1 -UseAzureManagedPrometheus

# 특정 네임스페이스 + Azure Managed Prometheus
.\run-diagnose.ps1 -UseAzureManagedPrometheus -Namespace ml-pipeline
```

```bash
# Linux / macOS — Azure Managed Prometheus 자동 연결
./run-diagnose.sh --use-azure-managed-prometheus

# 특정 네임스페이스 + Azure Managed Prometheus
./run-diagnose.sh --use-azure-managed-prometheus -n ml-pipeline
```

> 이 옵션을 사용하려면 `az` (Azure CLI) 가 설치되어 있고 `az login` 이 완료된 상태여야 합니다.
> Linux / macOS 에서는 JSON 파싱을 위해 `python3` 도 필요합니다.
> ```powershell
> # Windows
> winget install Microsoft.AzureCLI   # az 설치
> az login                             # Azure 로그인
> ```
> ```bash
> # Linux / macOS — 예: Ubuntu
> curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash   # az 설치
> az login                                                  # Azure 로그인
> ```

**자동으로 수행되는 작업 (최초 1회):**

| 단계 | 내용 |
|------|------|
| 1. AKS 감지 | 현재 `kubectl context` 에서 AKS 클러스터 자동 식별 |
| 2. OIDC / Workload Identity | 미활성 시 자동 활성화 (`az aks update`) |
| 3. Azure Monitor Workspace | 리소스 그룹 내 Workspace 자동 탐색, Prometheus 엔드포인트 추출 |
| 4. Managed Identity 생성 | `aks-diagnose-mi` 이름으로 생성 (이미 존재하면 재사용) |
| 5. 역할 부여 | Managed Identity 에 `Monitoring Data Reader` 권한 자동 부여 |
| 6. Federated Credential | K8s ServiceAccount(`aks-diagnose`) ↔ Managed Identity 연결 |
| 7. SA annotation | `azure.workload.identity/client-id` 자동 설정 |

> **Azure Monitor Workspace 가 여러 개인 경우** `-AzureMonitorWorkspaceName` (bash: `--azure-monitor-workspace-name`) 옵션으로 지정하세요.
> ```powershell
> .\run-diagnose.ps1 -UseAzureManagedPrometheus -AzureMonitorWorkspaceName my-amw
> ```
> ```bash
> ./run-diagnose.sh --use-azure-managed-prometheus --azure-monitor-workspace-name my-amw
> ```

> **이 옵션 없이도** 클러스터 내부에 직접 설치된 Prometheus(Helm 등)가 있으면 기본 URL 또는
> `-PrometheusUrl` 로 연결할 수 있습니다.


실행 흐름:
```
1. k8s/rbac.yaml 적용 (최초 1회)
2. 진단 Pod 생성 (aks_diagnose.py 실행)
3. 진단 완료 신호(__DIAG_DONE__) 대기
4. kubectl cp 로 HTML 리포트 로컬 복사
5. Pod 삭제
6. 브라우저로 리포트 자동 열기
```

---

### 방법 2.로컬 Python 으로 직접 실행

K8s 외부에서 실행하며, kubeconfig 의 현재 컨텍스트를 사용합니다.

```bash
# 의존성 설치
pip install -r requirements.txt

# 데모 모드 (클러스터 없이 샘플 데이터로 확인)
python aks_diagnose.py --demo --out report.html

# 실제 클러스터 진단 (kubeconfig 기반)
python aks_diagnose.py -A --out report.html

# Prometheus 연동
python aks_diagnose.py -A \
  --prometheus-url http://localhost:9090 \
  --out report.html
```

> 로컬 실행 시에는 kubeconfig 컨텍스트가 대상 클러스터를 가리켜야 합니다.

---

## HTML 리포트 구성

진단 완료 후 생성되는 `report_YYYYMMDD_HHMM.html` 파일은 단일 HTML 파일로 외부 의존성이 없습니다.

| 섹션 | 내용 |
|------|------|
| **헤더** | 위험도 점수 (0=안전 / 100=최악), 위험·주의·정보 건수 요약 |
| **컴포넌트 요약** | Deployment 별 파드 수 / 재시작 / CPU throttle / 메모리 카드 |
| **발견사항 및 권장 조치** | 심각도별 이슈 목록 + 상세 조치 단계 (펼치기/접기) |
| **트레이싱** | App Insights 컴포넌트별 · 홉별 latency / 실패율 (선택) |
| **Prometheus 신호** | 클러스터 CPU Throttle 추이 그래프 |
| **파드 상태** | 전체 파드 목록 — 상태 / ready / 재시작 / limits / 노드 |
| **이벤트** | Warning 이벤트 상위 15건 |
| **HPA** | HPA 현황 및 maxReplicas 포화 경고 |
| **스토리지 (PVC)** | PVC 상태 및 디스크 사용률 (Prometheus 연동 시) |
| **노드 상태** | 노드별 CPU·메모리 요청률 — 🟢 양호 / 🟡 주의 / 🔴 위험 |
| **Capacity Planning** | +20%·+50% 부하 시 예상 사용률 시뮬레이션 + 노드 현황 |
| **PDB** | PodDisruptionBudget 상태 및 노드 업그레이드 차단 위험 |

---

## 자세한 배포 가이드

-> [DEPLOY.md](DEPLOY.md)
