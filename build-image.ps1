# build-image.ps1
# aks-diagnose 이미지를 Azure ACR 에서 직접 빌드+push 합니다.
# Docker Desktop 불필요. az login 만 되어 있으면 됩니다.
#
# 사용법:
#   .\build-image.ps1
#   .\build-image.ps1 -Tag v1.2.3          # 특정 태그
#   .\build-image.ps1 -Registry myreg      # 다른 ACR

param(
    [string]$Registry = "factorykr",
    [string]$Image    = "aks-diagnose",
    [string]$Tag      = "latest"
)

$ErrorActionPreference = "Stop"
$FULL_IMAGE = "$Registry.azurecr.io/${Image}:${Tag}"

Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  AKS Diagnose - Image Build"               -ForegroundColor Cyan
Write-Host "  Registry : $Registry.azurecr.io"          -ForegroundColor Cyan
Write-Host "  Image    : ${Image}:${Tag}"                -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan

# az CLI 확인
if (-not (Get-Command az -ErrorAction SilentlyContinue)) {
    Write-Host "[FAIL] Azure CLI(az) not found." -ForegroundColor Red
    Write-Host "       Install: winget install Microsoft.AzureCLI" -ForegroundColor Red
    exit 1
}

# az acr build: 소스를 ACR 에 업로드 → 클라우드에서 빌드 → ACR 에 push
Write-Host "[INFO] ACR 빌드 시작 (클라우드 빌드)..." -ForegroundColor Cyan
$ErrorActionPreference = "Continue"   # az WARNING을 오류로 처리하지 않음
az acr build `
    --registry $Registry `
    --image "${Image}:${Tag}" `
    --platform linux/amd64 `
    --file Dockerfile `
    .
$buildExit = $LASTEXITCODE
$ErrorActionPreference = "Stop"

if ($buildExit -ne 0) {
    Write-Host "[FAIL] ACR 빌드 실패 (exit=$buildExit)" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "[OK] 빌드 완료: $FULL_IMAGE" -ForegroundColor Green
Write-Host ""
Write-Host "진단 실행:" -ForegroundColor Cyan
Write-Host "  .\run-diagnose.ps1" -ForegroundColor Gray
