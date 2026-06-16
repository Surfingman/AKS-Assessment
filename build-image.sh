#!/usr/bin/env bash
# build-image.sh
# aks-diagnose 이미지를 Azure ACR 에서 직접 빌드+push 합니다.
# Docker Desktop 불필요. az login 만 되어 있으면 됩니다.
#
# 사용법:
#   ./build-image.sh
#   ./build-image.sh -t v1.2.3          # 특정 태그
#   ./build-image.sh -r myreg           # 다른 ACR 레지스트리
#
# Windows 사용자는 build-image.ps1 을 사용하세요.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REGISTRY="factorykr"
IMAGE="aks-diagnose"
TAG="latest"

while [[ $# -gt 0 ]]; do
    case $1 in
        -r|--registry) REGISTRY="$2"; shift 2 ;;
        -i|--image)    IMAGE="$2";    shift 2 ;;
        -t|--tag)      TAG="$2";      shift 2 ;;
        -h|--help)
            sed -n '/^#/p' "$0" | head -15 | sed 's/^# \?//'
            exit 0 ;;
        *) echo "[FAIL] 알 수 없는 옵션: $1"; exit 1 ;;
    esac
done

FULL_IMAGE="${REGISTRY}.azurecr.io/${IMAGE}:${TAG}"

echo ""
echo "============================================"
echo "  AKS Diagnose - Image Build"
echo "  Registry : ${REGISTRY}.azurecr.io"
echo "  Image    : ${IMAGE}:${TAG}"
echo "============================================"

# az CLI 확인
if ! command -v az &>/dev/null; then
    echo "[FAIL] Azure CLI(az) 가 설치되지 않았습니다."
    echo "       설치: https://docs.microsoft.com/cli/azure/install-azure-cli"
    exit 1
fi

# az acr build: 소스를 ACR 에 업로드 → 클라우드에서 빌드 → ACR 에 push
echo "[INFO] ACR 빌드 시작 (클라우드 빌드)..."
az acr build \
    --registry "$REGISTRY" \
    --image "${IMAGE}:${TAG}" \
    --platform linux/amd64 \
    --file Dockerfile \
    "$SCRIPT_DIR"

echo ""
echo "[OK] 빌드 완료: $FULL_IMAGE"
echo ""
echo "진단 실행:"
echo "  ./run-diagnose.sh"
