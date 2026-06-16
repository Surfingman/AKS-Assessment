# 배포 가이드 - Kubernetes Pod 실행

aks_diagnose 를 Kubernetes 클러스터 내부에서 Pod 로 실행하고 HTML 리포트를 로컬로 가져오는 방법입니다.

OS별 스크립트:
- Windows  : .ps1 파일 사용 (build-image.ps1, run-diagnose.ps1)
- Linux/macOS : .sh 파일 사용 (build-image.sh, run-diagnose.sh)

---

## 사전 요구사항

| 항목 | 설명 |
|------|------|
| kubectl | 대상 AKS 클러스터에 연결된 상태 |
| az CLI | ACR 빌드에 필요 (az login 완료) |
| ACR 접근 권한 | az acr build 실행 권한 |

---

## 1단계 - 이미지 빌드 및 ACR Push

소스 코드(aks_diagnose.py)를 Azure Container Registry 에서 직접 빌드하고 push 합니다.
Docker Desktop 불필요 - 소스 코드만 업로드하면 Azure 클라우드에서 빌드됩니다.

### Windows

    .\build-image.ps1

기본값: Registry=factorykr, Image=aks-diagnose:latest

옵션 지정:

    .\build-image.ps1 -Registry myregistry -Image aks-diagnose -Tag v1.0.0

### Linux / macOS

    ./build-image.sh

옵션 지정:

    ./build-image.sh -r myregistry -i aks-diagnose -t v1.0.0

빌드 완료 출력 예시:

    [INFO] ACR 빌드 시작 (클라우드 빌드)...
    Run ID: abc was successful after 31s
    [OK] 빌드 완료: factorykr.azurecr.io/aks-diagnose:latest

---

## 2단계 - 진단 실행

클러스터 내부에 진단 Pod 를 생성하고 HTML 리포트를 로컬로 복사합니다.

### Windows

    # 전체 네임스페이스 진단 (기본)
    .\run-diagnose.ps1

    # 특정 네임스페이스만
    .\run-diagnose.ps1 -Namespace ml-pipeline

    # Prometheus 연동
    .\run-diagnose.ps1 -PrometheusUrl http://prom-server.monitoring.svc.cluster.local:80

    # 출력 파일 지정
    .\run-diagnose.ps1 -Out C:\reports\report.html

### Linux / macOS

    # 전체 네임스페이스 진단
    ./run-diagnose.sh

    # 특정 네임스페이스만
    ./run-diagnose.sh -n ml-pipeline

    # Prometheus 연동
    ./run-diagnose.sh -p http://prom-server.monitoring.svc.cluster.local:80

    # 출력 파일 지정
    ./run-diagnose.sh -o /tmp/report.html

### 실행 흐름

    1. k8s/rbac.yaml 적용 (ServiceAccount + ClusterRole 생성, 최초 1회)
    2. 진단 Pod 생성 및 aks_diagnose.py 실행
    3. 로그에서 __DIAG_DONE__ 신호 대기 (최대 240초)
    4. kubectl cp 로 /tmp/report.html 을 로컬로 복사
    5. Pod 삭제
    6. 브라우저로 리포트 자동 열기

정상 완료 출력 예시:

    [OK]   Pod created
    [INFO] 진단 완료 대기 중...
      Pod 시작 대기 중... (5 / 240 초) [phase: Pending]
      대기 중... (10 / 240 초) [phase: Running]
    [OK]   진단 완료
    [INFO] 리포트 추출 중 (kubectl cp)...
    [OK]   Pod 정리 중...
    [OK] 완료! Report: report_20260615_1300.html (41.5 KB)

---

## RBAC 설명

k8s/rbac.yaml 은 진단 Pod 가 K8s API 를 읽기 전용으로 조회하는 데 필요한 권한입니다.
ACR 이미지 pull 과는 무관하며, 클러스터 진단 데이터 수집에 반드시 필요합니다.

    ServiceAccount: aks-diagnose (namespace: default)
    ClusterRole:    aks-diagnose-readonly
      - pods, nodes, events, persistentvolumeclaims  -> get, list
      - deployments, replicasets, statefulsets        -> get, list
      - horizontalpodautoscalers                      -> get, list
      - poddisruptionbudgets                          -> get, list
      - jobs, cronjobs                                -> get, list

---

## 문제 해결

### Pod 가 Failed 상태로 종료되는 경우

스크립트가 자동으로 kubectl describe 와 kubectl logs 를 출력합니다.

| 원인 | 확인 방법 |
|------|-----------|
| 이미지 pull 실패 | kubectl describe pod 의 Events 섹션 |
| RBAC 권한 부족 | kubectl logs 의 Permission denied |
| Python 스크립트 오류 | 로그의 __DIAG_FAILED__ + 스택 트레이스 |

### 타임아웃 (240초 초과)

    # Windows
    kubectl get pods | Select-String aks-diagnose
    kubectl logs <pod-name>
    kubectl describe pod <pod-name>

    # Linux / macOS
    kubectl get pods | grep aks-diagnose
    kubectl logs <pod-name>
    kubectl describe pod <pod-name>
