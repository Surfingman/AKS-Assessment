#!/usr/bin/env bash
# run-diagnose.sh
# AKS 진단 도구 실행 스크립트 (Linux / macOS)
# Pod YAML 로 이미지를 실행하고 kubectl cp 로 HTML 리포트를 로컬로 복사합니다.
# Python/Docker 설치 불필요. kubectl 만 있으면 됩니다.
#
# 사용법:
#   ./run-diagnose.sh
#   ./run-diagnose.sh -n ml-pipeline
#   ./run-diagnose.sh -n ml-pipeline -o /tmp/report.html
#   ./run-diagnose.sh -p http://my-prometheus:9090
#   ./run-diagnose.sh --use-azure-managed-prometheus
#   ./run-diagnose.sh --use-azure-managed-prometheus --azure-monitor-workspace-name my-amw
#
# Windows 사용자는 run-diagnose.ps1 을 사용하세요.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
NAMESPACE=""
PROM_URL="http://prom-prometheus-server.monitoring.svc.cluster.local:80"
IMAGE="factorykr.azurecr.io/aks-diagnose:latest"
OUT="${SCRIPT_DIR}/report_$(date +%Y%m%d_%H%M).html"
LOCAL_IMAGE=false
USE_AZURE_MANAGED_PROMETHEUS=false
MANAGED_IDENTITY_NAME="aks-diagnose-mi"
AZURE_MONITOR_WORKSPACE_NAME=""
POD_NAMESPACE="default"
SERVICE_ACCOUNT="aks-diagnose"
PROMETHEUS_AAD=false

while [[ $# -gt 0 ]]; do
    case $1 in
        -n|--namespace)                      NAMESPACE="$2";                     shift 2 ;;
        -o|--out)                            OUT="$2";                           shift 2 ;;
        -p|--prometheus)                     PROM_URL="$2";                      shift 2 ;;
        -i|--image)                          IMAGE="$2";                         shift 2 ;;
        --local-image)                       LOCAL_IMAGE=true;                   shift ;;
        --use-azure-managed-prometheus)      USE_AZURE_MANAGED_PROMETHEUS=true;  shift ;;
        --managed-identity-name)             MANAGED_IDENTITY_NAME="$2";        shift 2 ;;
        --azure-monitor-workspace-name)      AZURE_MONITOR_WORKSPACE_NAME="$2"; shift 2 ;;
        -h|--help)
            sed -n '/^#/p' "$0" | head -20 | sed 's/^# \?//'
            exit 0 ;;
        *) echo "[FAIL] 알 수 없는 옵션: $1"; exit 1 ;;
    esac
done

POD_NAME="aks-diagnose-$(date +%H%M%S)"

# ── 헬퍼 함수 ─────────────────────────────────────────────────────────────

require_command() {
    local name="$1" hint="$2"
    if ! command -v "$name" &>/dev/null; then
        echo "[FAIL] $name not found. $hint"
        exit 1
    fi
}

get_current_aks_from_kubectl_context() {
    local server
    server=$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')
    if [[ -z "$server" ]]; then
        echo "[FAIL] kubectl context에서 cluster.server를 읽지 못했습니다." >&2
        exit 1
    fi

    local aks_list
    aks_list=$(az aks list -o json)

    # fqdn 또는 privateFqdn 으로 매칭
    local rg name
    rg=$(echo "$aks_list" | python3 -c "
import json,sys
clusters=json.load(sys.stdin)
server='$server'
for c in clusters:
    if (c.get('fqdn') and c['fqdn'] in server) or (c.get('privateFqdn') and c['privateFqdn'] in server):
        print(c['resourceGroup']); break
")
    name=$(echo "$aks_list" | python3 -c "
import json,sys
clusters=json.load(sys.stdin)
server='$server'
for c in clusters:
    if (c.get('fqdn') and c['fqdn'] in server) or (c.get('privateFqdn') and c['privateFqdn'] in server):
        print(c['name']); break
")

    if [[ -z "$rg" || -z "$name" ]]; then
        echo "[FAIL] 현재 kubectl context와 일치하는 AKS를 찾지 못했습니다." >&2
        exit 1
    fi

    echo "$rg|$name"
}

ensure_azure_managed_prometheus() {
    echo "[INFO] Azure Managed Prometheus 자동 설정..."

    local aks_info
    aks_info=$(get_current_aks_from_kubectl_context)
    local RESOURCE_GROUP NAME
    RESOURCE_GROUP="${aks_info%%|*}"
    NAME="${aks_info##*|}"

    echo "[INFO] AKS detected: $RESOURCE_GROUP / $NAME"

    local aks_json issuer wi_enabled
    aks_json=$(az aks show --resource-group "$RESOURCE_GROUP" --name "$NAME" -o json)
    issuer=$(echo "$aks_json" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('oidcIssuerProfile',{}).get('issuerUrl',''))")
    wi_enabled=$(echo "$aks_json" | python3 -c "import json,sys; d=json.load(sys.stdin); sp=d.get('securityProfile',{}); wi=sp.get('workloadIdentity',{}); print('true' if wi.get('enabled') else 'false')")

    if [[ -z "$issuer" || "$wi_enabled" == "false" ]]; then
        echo "[INFO] OIDC / Workload Identity 활성화 중..."
        az aks update \
            --resource-group "$RESOURCE_GROUP" \
            --name "$NAME" \
            --enable-oidc-issuer \
            --enable-workload-identity > /dev/null
        aks_json=$(az aks show --resource-group "$RESOURCE_GROUP" --name "$NAME" -o json)
        issuer=$(echo "$aks_json" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('oidcIssuerProfile',{}).get('issuerUrl',''))")
    fi

    if [[ -z "$issuer" ]]; then
        echo "[FAIL] OIDC issuer URL을 가져오지 못했습니다." >&2
        exit 1
    fi

    local workspace_json prom_url workspace_id
    if [[ -z "$AZURE_MONITOR_WORKSPACE_NAME" ]]; then
        workspace_json=$(az monitor account list --resource-group "$RESOURCE_GROUP" -o json | python3 -c "import json,sys; lst=json.load(sys.stdin); print(json.dumps(lst[0])) if lst else (sys.stderr.write('Azure Monitor Workspace를 찾지 못했습니다.\n') or sys.exit(1))")
    else
        workspace_json=$(az monitor account show --resource-group "$RESOURCE_GROUP" --name "$AZURE_MONITOR_WORKSPACE_NAME" -o json)
    fi

    prom_url=$(echo "$workspace_json" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('metrics',{}).get('prometheusQueryEndpoint',''))")
    workspace_id=$(echo "$workspace_json" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('id',''))")
    local workspace_name
    workspace_name=$(echo "$workspace_json" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('name',''))")

    if [[ -z "$prom_url" ]]; then
        echo "[FAIL] prometheusQueryEndpoint를 찾지 못했습니다." >&2
        exit 1
    fi

    echo "[INFO] Azure Monitor Workspace: $workspace_name"
    echo "[INFO] Prometheus endpoint: $prom_url"

    # Managed Identity 생성/확인
    local mi_json client_id principal_id
    if ! az identity show --resource-group "$RESOURCE_GROUP" --name "$MANAGED_IDENTITY_NAME" -o json &>/dev/null; then
        echo "[INFO] Managed Identity 생성: $MANAGED_IDENTITY_NAME"
        az identity create --resource-group "$RESOURCE_GROUP" --name "$MANAGED_IDENTITY_NAME" > /dev/null
    else
        echo "[OK]   Managed Identity exists: $MANAGED_IDENTITY_NAME"
    fi

    mi_json=$(az identity show --resource-group "$RESOURCE_GROUP" --name "$MANAGED_IDENTITY_NAME" -o json)
    client_id=$(echo "$mi_json" | python3 -c "import json,sys; print(json.load(sys.stdin)['clientId'])")
    principal_id=$(echo "$mi_json" | python3 -c "import json,sys; print(json.load(sys.stdin)['principalId'])")

    echo "[INFO] Managed Identity clientId: $client_id"

    # 역할 부여
    local role_count
    role_count=$(az role assignment list \
        --assignee-object-id "$principal_id" \
        --scope "$workspace_id" \
        --query "[?roleDefinitionName=='Monitoring Data Reader'] | length(@)" \
        -o tsv)

    if [[ "$role_count" -eq 0 ]]; then
        echo "[INFO] Monitoring Data Reader 권한 부여..."
        az role assignment create \
            --assignee-object-id "$principal_id" \
            --assignee-principal-type ServicePrincipal \
            --role "Monitoring Data Reader" \
            --scope "$workspace_id" > /dev/null
    else
        echo "[OK]   Monitoring Data Reader already assigned"
    fi

    # Federated Credential
    local subject="system:serviceaccount:${POD_NAMESPACE}:${SERVICE_ACCOUNT}"
    local fed_exists
    fed_exists=$(az identity federated-credential list \
        --resource-group "$RESOURCE_GROUP" \
        --identity-name "$MANAGED_IDENTITY_NAME" \
        -o json | python3 -c "
import json,sys
lst=json.load(sys.stdin)
issuer='$issuer'; subject='$subject'
print('true' if any(f.get('issuer')==issuer and f.get('subject')==subject for f in lst) else 'false')
")

    if [[ "$fed_exists" == "false" ]]; then
        echo "[INFO] Federated Credential 생성..."
        az identity federated-credential create \
            --resource-group "$RESOURCE_GROUP" \
            --identity-name "$MANAGED_IDENTITY_NAME" \
            --name "aks-diagnose-fed" \
            --issuer "$issuer" \
            --subject "$subject" > /dev/null
    else
        echo "[OK]   Federated Credential already exists"
    fi

    # ServiceAccount annotation
    echo "[INFO] ServiceAccount annotation 업데이트..."
    kubectl annotate sa "$SERVICE_ACCOUNT" -n "$POD_NAMESPACE" \
        "azure.workload.identity/client-id=${client_id}" \
        --overwrite > /dev/null

    # 결과 반환 (전역 변수로)
    PROM_URL="$prom_url"
    PROMETHEUS_AAD=true
    echo "[OK]   Azure Managed Prometheus 설정 완료"
}

# ── 메인 ──────────────────────────────────────────────────────────────────

echo ""
echo "============================================"
echo "  AKS Diagnose Tool"
echo "  Image : $IMAGE"
echo "  Output: $OUT"
echo "============================================"

require_command "kubectl" "Install: https://kubernetes.io/docs/tasks/tools/"

if [[ "$USE_AZURE_MANAGED_PROMETHEUS" == "true" ]]; then
    require_command "az" "Install: https://docs.microsoft.com/cli/azure/install-azure-cli"
    require_command "python3" "Install: https://www.python.org/"
fi

echo "[INFO] Context: $(kubectl config current-context)"

# 1. RBAC 적용
echo "[INFO] Applying RBAC..."
kubectl apply -f "${SCRIPT_DIR}/k8s/rbac.yaml" > /dev/null
echo "[OK]   RBAC ready"

# 2. Azure Managed Prometheus 자동 설정 (옵션)
if [[ "$USE_AZURE_MANAGED_PROMETHEUS" == "true" ]]; then
    ensure_azure_managed_prometheus
fi

# 3. 진단 인수 구성
PY_ARGS="--in-cluster --prometheus-url ${PROM_URL} --out /tmp/report.html --no-history"

if [[ "$PROMETHEUS_AAD" == "true" ]]; then
    PY_ARGS="${PY_ARGS} --prometheus-aad"
fi

if [[ -n "$NAMESPACE" ]]; then
    PY_ARGS="-n ${NAMESPACE} ${PY_ARGS}"
    echo "[INFO] Target: namespace=$NAMESPACE"
else
    PY_ARGS="-A ${PY_ARGS}"
    echo "[INFO] Target: all namespaces (-A)"
fi

echo "[INFO] PrometheusUrl: $PROM_URL"
echo "[INFO] PrometheusAad: $PROMETHEUS_AAD"

# 4. Pod 생성
echo "[INFO] Running pod: $POD_NAME ..."
echo "[INFO] 진단 중입니다. 1~2분 소요됩니다..."

FULL_CMD="python aks_diagnose.py ${PY_ARGS} && echo __DIAG_DONE__ || echo __DIAG_FAILED__; sleep 120"

if [[ "$LOCAL_IMAGE" == "true" ]]; then
    PULL_POLICY="Never"
else
    PULL_POLICY="Always"
fi

# Workload Identity 사용 시 label 추가
if [[ "$USE_AZURE_MANAGED_PROMETHEUS" == "true" ]]; then
    WI_LABEL="  labels:\n    azure.workload.identity/use: \"true\""
else
    WI_LABEL=""
fi

TMP_YAML=$(mktemp /tmp/aks-diag-XXXXXX.yaml)
printf 'apiVersion: v1\nkind: Pod\nmetadata:\n  name: %s\n  namespace: %s\n%s\nspec:\n  serviceAccountName: %s\n  restartPolicy: Never\n  containers:\n  - name: aks-diagnose\n    image: %s\n    imagePullPolicy: %s\n    command:\n    - sh\n    - -c\n    - '"'"'%s'"'"'\n    resources:\n      requests:\n        cpu: 100m\n        memory: 128Mi\n      limits:\n        cpu: 1000m\n        memory: 512Mi\n    volumeMounts:\n    - name: tmp-vol\n      mountPath: /tmp\n  volumes:\n  - name: tmp-vol\n    emptyDir: {}\n' \
    "$POD_NAME" "$POD_NAMESPACE" "$(printf '%b' "$WI_LABEL")" \
    "$SERVICE_ACCOUNT" "$IMAGE" "$PULL_POLICY" "$FULL_CMD" > "$TMP_YAML"

kubectl apply -f "$TMP_YAML" > /dev/null
rm -f "$TMP_YAML"
echo "[OK]   Pod created"

# 5. 진단 완료/실패 신호 대기
echo "[INFO] 진단 완료 대기 중..."
TIMEOUT=240
ELAPSED=0
DONE=false

while [[ $ELAPSED -lt $TIMEOUT ]]; do
    PHASE=$(kubectl get pod "$POD_NAME" -n "$POD_NAMESPACE" -o jsonpath='{.status.phase}' 2>/dev/null || echo "")

    if [[ "$PHASE" == "Failed" ]]; then
        echo "[FAIL] Pod 실패!"
        echo ""
        echo "=== kubectl describe pod $POD_NAME ==="
        kubectl describe pod "$POD_NAME" -n "$POD_NAMESPACE" 2>&1 || true
        echo ""
        echo "=== kubectl logs $POD_NAME ==="
        kubectl logs "$POD_NAME" -n "$POD_NAMESPACE" 2>&1 || true
        echo ""
        kubectl delete pod "$POD_NAME" -n "$POD_NAMESPACE" --ignore-not-found > /dev/null 2>&1 || true
        exit 1
    fi

    if [[ "$PHASE" != "Running" ]]; then
        sleep 5
        ELAPSED=$((ELAPSED + 5))
        echo "  Pod 시작 대기 중... ($ELAPSED / $TIMEOUT 초) [phase: $PHASE]"
        continue
    fi

    LOGS=$(kubectl logs "$POD_NAME" -n "$POD_NAMESPACE" 2>/dev/null || echo "")

    if echo "$LOGS" | grep -q '__DIAG_FAILED__'; then
        echo "[FAIL] 진단 스크립트 오류. Pod 로그:"
        echo "$LOGS"
        echo ""
        echo "[INFO] 디버그 명령어 (pod 가 곧 삭제됩니다):"
        echo "       kubectl logs $POD_NAME -n $POD_NAMESPACE"
        echo "       kubectl exec -it $POD_NAME -n $POD_NAMESPACE -- sh"
        kubectl delete pod "$POD_NAME" -n "$POD_NAMESPACE" --ignore-not-found > /dev/null 2>&1 || true
        exit 1
    fi

    if echo "$LOGS" | grep -q '__DIAG_DONE__'; then
        DONE=true
        break
    fi

    sleep 5
    ELAPSED=$((ELAPSED + 5))
    echo "  대기 중... ($ELAPSED / $TIMEOUT 초) [phase: $PHASE]"
done

if [[ "$DONE" != "true" ]]; then
    echo "[FAIL] 타임아웃 (${TIMEOUT}초). Pod 로그:"
    kubectl logs "$POD_NAME" -n "$POD_NAMESPACE" 2>&1 || true
    echo ""
    echo "[INFO] 디버그 명령어:"
    echo "       kubectl logs $POD_NAME -n $POD_NAMESPACE"
    echo "       kubectl describe pod $POD_NAME -n $POD_NAMESPACE"
    kubectl delete pod "$POD_NAME" -n "$POD_NAMESPACE" --ignore-not-found > /dev/null 2>&1 || true
    exit 1
fi
echo "[OK]   진단 완료"

# 6. kubectl cp 로 HTML 파일 복사 (UTF-8 바이너리 그대로 전송)
echo "[INFO] 리포트 추출 중 (kubectl cp)..."
TMP_NAME="aks-tmp-${POD_NAME}.html"
TMP_FULL="${SCRIPT_DIR}/${TMP_NAME}"

pushd "$SCRIPT_DIR" > /dev/null
if ! kubectl cp "${POD_NAMESPACE}/${POD_NAME}:/tmp/report.html" "$TMP_NAME" 2>&1; then
    echo "[FAIL] kubectl cp 실패"
    rm -f "$TMP_FULL"
    kubectl delete pod "$POD_NAME" -n "$POD_NAMESPACE" --ignore-not-found > /dev/null 2>&1 || true
    popd > /dev/null
    exit 1
fi
popd > /dev/null

mv "$TMP_FULL" "$OUT"

echo "[OK]   Pod 정리 중..."
kubectl delete pod "$POD_NAME" -n "$POD_NAMESPACE" --ignore-not-found > /dev/null 2>&1 || true

# 7. 결과 열기
if [[ -f "$OUT" ]]; then
    SIZE=$(du -sh "$OUT" | cut -f1)
    echo ""
    echo "[OK] 완료! Report: $OUT ($SIZE)"
    if command -v xdg-open &>/dev/null; then
        xdg-open "$OUT" &
    elif command -v open &>/dev/null; then
        open "$OUT"
    else
        echo "     브라우저로 직접 열어주세요: $OUT"
    fi
else
    echo "[FAIL] 파일 복사 실패."
    exit 1
fi