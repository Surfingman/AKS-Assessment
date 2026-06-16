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
#
# Windows 사용자는 run-diagnose.ps1 을 사용하세요.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
NAMESPACE=""
PROM_URL="http://prom-prometheus-server.monitoring.svc.cluster.local:80"
IMAGE="factorykr.azurecr.io/aks-diagnose:latest"
OUT="${SCRIPT_DIR}/report_$(date +%Y%m%d_%H%M).html"
LOCAL_IMAGE=false

while [[ $# -gt 0 ]]; do
    case $1 in
        -n|--namespace)   NAMESPACE="$2";    shift 2 ;;
        -o|--out)         OUT="$2";          shift 2 ;;
        -p|--prometheus)  PROM_URL="$2";     shift 2 ;;
        -i|--image)       IMAGE="$2";        shift 2 ;;
        --local-image)    LOCAL_IMAGE=true;  shift ;;
        -h|--help)
            sed -n '/^#/p' "$0" | head -15 | sed 's/^# \?//'
            exit 0 ;;
        *) echo "[FAIL] 알 수 없는 옵션: $1"; exit 1 ;;
    esac
done

POD_NAME="aks-diagnose-$(date +%H%M%S)"

echo ""
echo "============================================"
echo "  AKS Diagnose Tool"
echo "  Image : $IMAGE"
echo "  Output: $OUT"
echo "============================================"

# 1. kubectl 확인
if ! command -v kubectl &>/dev/null; then
    echo "[FAIL] kubectl not found."
    echo "       Install: https://kubernetes.io/docs/tasks/tools/"
    exit 1
fi
echo "[INFO] Context: $(kubectl config current-context)"

# 2. RBAC 적용 (최초 1회만 필요, 이후는 no-op)
echo "[INFO] Applying RBAC..."
kubectl apply -f "${SCRIPT_DIR}/k8s/rbac.yaml" > /dev/null
echo "[OK]   RBAC ready"

# 3. 진단 인수 구성
PY_ARGS="--in-cluster --prometheus-url ${PROM_URL} --out /tmp/report.html --no-history"
if [[ -n "$NAMESPACE" ]]; then
    PY_ARGS="-n ${NAMESPACE} ${PY_ARGS}"
    echo "[INFO] Target: namespace=$NAMESPACE"
else
    PY_ARGS="-A ${PY_ARGS}"
    echo "[INFO] Target: all namespaces (-A)"
fi

# 4. Pod YAML 생성 후 실행
# 전략: 진단 완료 후 120초 sleep → Running 상태 유지 → kubectl cp 수행 → pod 삭제
# (Completed/Terminated pod 에는 kubectl cp 불가)
echo "[INFO] Running pod: $POD_NAME ..."
echo "[INFO] 진단 중입니다. 1~2분 소요됩니다..."

# '||' 로 실패 신호도 캡처. ';sleep 120' 으로 성공/실패 무관하게 pod 를 120초 유지
FULL_CMD="python aks_diagnose.py ${PY_ARGS} && echo __DIAG_DONE__ || echo __DIAG_FAILED__; sleep 120"

# 로컬 이미지 사용 시 imagePullPolicy: Never
if [[ "$LOCAL_IMAGE" == "true" ]]; then
    PULL_POLICY="Never"
else
    PULL_POLICY="Always"
fi

# 임시 YAML 파일 생성
TMP_YAML=$(mktemp /tmp/aks-diag-XXXXXX.yaml)
cat > "$TMP_YAML" <<YAML
apiVersion: v1
kind: Pod
metadata:
  name: ${POD_NAME}
  namespace: default
spec:
  serviceAccountName: aks-diagnose
  restartPolicy: Never
  containers:
  - name: aks-diagnose
    image: ${IMAGE}
    imagePullPolicy: ${PULL_POLICY}
    command:
    - sh
    - -c
    - '${FULL_CMD}'
    resources:
      requests:
        cpu: 100m
        memory: 128Mi
      limits:
        cpu: 1000m
        memory: 512Mi
    volumeMounts:
    - name: tmp-vol
      mountPath: /tmp
  volumes:
  - name: tmp-vol
    emptyDir: {}
YAML

kubectl apply -f "$TMP_YAML" > /dev/null
rm -f "$TMP_YAML"
echo "[OK]   Pod created"

# 5. 진단 완료/실패 신호 대기
echo "[INFO] 진단 완료 대기 중..."
TIMEOUT=240
ELAPSED=0
DONE=false

while [[ $ELAPSED -lt $TIMEOUT ]]; do
    PHASE=$(kubectl get pod "$POD_NAME" -o jsonpath='{.status.phase}' 2>/dev/null || echo "")

    if [[ "$PHASE" == "Failed" ]]; then
        echo "[FAIL] Pod 실패!"
        echo ""
        echo "=== kubectl describe pod $POD_NAME ==="
        kubectl describe pod "$POD_NAME" 2>&1 || true
        echo ""
        echo "=== kubectl logs $POD_NAME ==="
        kubectl logs "$POD_NAME" 2>&1 || true
        echo ""
        kubectl delete pod "$POD_NAME" --ignore-not-found > /dev/null 2>&1 || true
        exit 1
    fi

    # ContainerCreating / Pending 등 아직 시작 전이면 로그 조회 건너뜀
    if [[ "$PHASE" != "Running" ]]; then
        sleep 5
        ELAPSED=$((ELAPSED + 5))
        echo "  Pod 시작 대기 중... ($ELAPSED / $TIMEOUT 초) [phase: $PHASE]"
        continue
    fi

    # Running 상태일 때만 로그 확인
    LOGS=$(kubectl logs "$POD_NAME" 2>/dev/null || echo "")

    if echo "$LOGS" | grep -q '__DIAG_FAILED__'; then
        echo "[FAIL] 진단 스크립트 오류. Pod 로그:"
        echo "$LOGS"
        echo ""
        echo "[INFO] 디버그 명령어 (pod 가 곧 삭제됩니다):"
        echo "       kubectl logs $POD_NAME"
        echo "       kubectl exec -it $POD_NAME -- sh"
        kubectl delete pod "$POD_NAME" --ignore-not-found > /dev/null 2>&1 || true
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
    kubectl logs "$POD_NAME" 2>&1 || true
    echo ""
    echo "[INFO] 디버그 명령어:"
    echo "       kubectl logs $POD_NAME"
    echo "       kubectl describe pod $POD_NAME"
    kubectl delete pod "$POD_NAME" --ignore-not-found > /dev/null 2>&1 || true
    exit 1
fi
echo "[OK]   진단 완료"

# 6. kubectl cp 로 HTML 파일을 로컬로 복사 (UTF-8 바이너리 그대로 전송)
echo "[INFO] 리포트 추출 중 (kubectl cp)..."
if ! kubectl cp "default/${POD_NAME}:/tmp/report.html" "$OUT" 2>&1; then
    echo "[FAIL] kubectl cp 실패"
    kubectl delete pod "$POD_NAME" --ignore-not-found > /dev/null 2>&1 || true
    exit 1
fi

echo "[OK]   Pod 정리 중..."
kubectl delete pod "$POD_NAME" --ignore-not-found > /dev/null 2>&1 || true

# 7. 결과 열기
if [[ -f "$OUT" ]]; then
    SIZE=$(du -sh "$OUT" | cut -f1)
    echo ""
    echo "[OK] 완료! Report: $OUT ($SIZE)"
    # 브라우저로 열기
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
