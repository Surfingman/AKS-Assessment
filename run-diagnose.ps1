# run-diagnose.ps1
# AKS 진단 도구 실행 스크립트
# Pod YAML 로 이미지를 실행하고 kubectl cp 로 HTML 리포트를 로컬로 복사합니다.
# Python/Docker 설치 불필요. kubectl 만 있으면 됩니다.
#
# 사용법:
#   .\run-diagnose.ps1
#   .\run-diagnose.ps1 -Namespace ml-pipeline
#   .\run-diagnose.ps1 -Namespace ml-pipeline -Out C:\reports\report.html
#   .\run-diagnose.ps1 -PrometheusUrl http://my-prometheus:9090

param(
    [string]$Namespace     = "",
    [string]$PrometheusUrl = "http://prom-prometheus-server.monitoring.svc.cluster.local:80",
    [string]$Image         = "factorykr.azurecr.io/aks-diagnose:latest",
    [switch]$LocalImage,
    [string]$Out           = ""
)

if ($Out -eq "") {
    $ts  = Get-Date -Format "yyyyMMdd_HHmm"
    $Out = "$PSScriptRoot\report_$ts.html"
}

$ErrorActionPreference = "Stop"
$POD_NAME   = "aks-diagnose-$(Get-Date -Format 'HHmmss')"
$SCRIPT_DIR = $PSScriptRoot

Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  AKS Diagnose Tool"                        -ForegroundColor Cyan
Write-Host "  Image : $Image"                           -ForegroundColor Cyan
Write-Host "  Output: $Out"                             -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan

# 1. kubectl 확인
if (-not (Get-Command kubectl -ErrorAction SilentlyContinue)) {
    Write-Host "[FAIL] kubectl not found." -ForegroundColor Red
    Write-Host "       Install: winget install Kubernetes.kubectl" -ForegroundColor Red
    exit 1
}
Write-Host "[INFO] Context: $(kubectl config current-context)" -ForegroundColor Cyan

# 2. RBAC 적용 (최초 1회만 필요, 이후는 no-op)
Write-Host "[INFO] Applying RBAC..." -ForegroundColor Cyan
kubectl apply -f "$SCRIPT_DIR\k8s\rbac.yaml" | Out-Null
Write-Host "[OK]   RBAC ready" -ForegroundColor Green

# 3. 진단 인수 구성 (파일로 직접 저장)
$pyArgs = @("--in-cluster", "--prometheus-url", $PrometheusUrl, "--out", "/tmp/report.html", "--no-history")
if ($Namespace -ne "") {
    $pyArgs = @("-n", $Namespace) + $pyArgs
    Write-Host "[INFO] Target: namespace=$Namespace" -ForegroundColor Cyan
} else {
    $pyArgs = @("-A") + $pyArgs
    Write-Host "[INFO] Target: all namespaces (-A)" -ForegroundColor Cyan
}

# 4. Pod YAML 생성 후 실행
# 전략: 진단 완료 후 300초(5분) sleep → Running 상태 유지 → kubectl cp 수행 → pod 삭제
# (Completed/Terminated pod 에는 kubectl cp 불가)
Write-Host "[INFO] Running pod: $POD_NAME ..." -ForegroundColor Cyan
Write-Host "[INFO] 진단 중입니다. 1~2분 소요됩니다..." -ForegroundColor Gray

# sh -c 로 한 줄 명령어로 실행
# '||' 로 실패 신호도 캡처. ';sleep 120' 으로 성공/실패 무관하게 pod 를 120초 유지 → kubectl cp 가능
$fullCmd = "python aks_diagnose.py $(($pyArgs | ForEach-Object { $_ }) -join ' ') && echo __DIAG_DONE__ || echo __DIAG_FAILED__; sleep 120"

# 로컬 이미지 사용 시 imagePullPolicy: Never (ACR pull 불필요)
$pullPolicy = if ($LocalImage) { "Never" } else { "Always" }

# YAML을 직접 WriteAllText로 생성 (here-string 파싱 문제 회피)
$yaml = @()
$yaml += "apiVersion: v1"
$yaml += "kind: Pod"
$yaml += "metadata:"
$yaml += "  name: $POD_NAME"
$yaml += "  namespace: default"
$yaml += "spec:"
$yaml += "  serviceAccountName: aks-diagnose"
$yaml += "  restartPolicy: Never"
$yaml += "  containers:"
$yaml += "  - name: aks-diagnose"
$yaml += "    image: $Image"
$yaml += "    imagePullPolicy: $pullPolicy"
$yaml += "    command:"
$yaml += "    - sh"
$yaml += "    - -c"
$yaml += "    - '$fullCmd'"
$yaml += "    resources:"
$yaml += "      requests:"
$yaml += "        cpu: 100m"
$yaml += "        memory: 128Mi"
$yaml += "      limits:"
$yaml += "        cpu: 1000m"
$yaml += "        memory: 512Mi"
$yaml += "    volumeMounts:"
$yaml += "    - name: tmp-vol"
$yaml += "      mountPath: /tmp"
$yaml += "  volumes:"
$yaml += "  - name: tmp-vol"
$yaml += "    emptyDir: {}"

$tmpYaml = "$env:TEMP\aks-diag-$POD_NAME.yaml"
($yaml -join "`n") | Out-File -FilePath $tmpYaml -Encoding ascii -NoNewline

# YAML 내용 확인 (디버그용 - 문제시 주석 해제)
# Get-Content $tmpYaml

kubectl apply -f $tmpYaml | Out-Null
Remove-Item $tmpYaml -ErrorAction SilentlyContinue
Write-Host "[OK]   Pod created" -ForegroundColor Green

# 5. 진단 완료/실패 신호 대기 (pod 는 sleep 120 으로 Running 상태 유지)
Write-Host "[INFO] 진단 완료 대기 중..." -ForegroundColor Cyan
$timeout = 240; $elapsed = 0; $done = $false
while ($elapsed -lt $timeout) {
    # kubectl stderr 가 $ErrorActionPreference=Stop 에 걸리지 않도록 Continue 로 임시 전환
    $ErrorActionPreference = "Continue"
    $phase = (kubectl get pod $POD_NAME -o jsonpath='{.status.phase}' 2>$null)
    $ErrorActionPreference = "Stop"

    if ($phase -eq "Failed") {
        Write-Host "[FAIL] Pod 실패!" -ForegroundColor Red
        Write-Host ""
        Write-Host "=== kubectl describe pod $POD_NAME ===" -ForegroundColor Yellow
        $ErrorActionPreference = "Continue"
        kubectl describe pod $POD_NAME 2>&1
        Write-Host ""
        Write-Host "=== kubectl logs $POD_NAME ===" -ForegroundColor Yellow
        kubectl logs $POD_NAME 2>&1
        $ErrorActionPreference = "Stop"
        Write-Host ""
        kubectl delete pod $POD_NAME --ignore-not-found | Out-Null
        exit 1
    }

    # ContainerCreating / Pending 등 아직 시작 전이면 로그 조회 건너뜀
    if ($phase -ne "Running") {
        Start-Sleep -Seconds 5; $elapsed += 5
        Write-Host "  Pod 시작 대기 중... ($elapsed / $timeout 초) [phase: $phase]" -ForegroundColor Gray
        continue
    }

    # Running 상태일 때만 로그 확인
    $ErrorActionPreference = "Continue"
    $logs = (kubectl logs $POD_NAME 2>$null)
    $ErrorActionPreference = "Stop"

    if ($logs -match '__DIAG_FAILED__') {
        Write-Host "[FAIL] 진단 스크립트 오류. Pod 로그:" -ForegroundColor Red
        Write-Host $logs -ForegroundColor Yellow
        Write-Host ""
        Write-Host "[INFO] 디버그 명령어 (pod 가 곧 삭제됩니다):" -ForegroundColor Cyan
        Write-Host "       kubectl logs $POD_NAME" -ForegroundColor Gray
        Write-Host "       kubectl exec -it $POD_NAME -- sh" -ForegroundColor Gray
        kubectl delete pod $POD_NAME --ignore-not-found | Out-Null
        exit 1
    }
    if ($logs -match '__DIAG_DONE__') { $done = $true; break }

    Start-Sleep -Seconds 5; $elapsed += 5
    Write-Host "  대기 중... ($elapsed / $timeout 초) [phase: $phase]" -ForegroundColor Gray
}

if (-not $done) {
    Write-Host "[FAIL] 타임아웃 ($timeout 초). Pod 로그:" -ForegroundColor Red
    kubectl logs $POD_NAME 2>&1
    Write-Host ""
    Write-Host "[INFO] 디버그 명령어:" -ForegroundColor Cyan
    Write-Host "       kubectl logs $POD_NAME" -ForegroundColor Gray
    Write-Host "       kubectl describe pod $POD_NAME" -ForegroundColor Gray
    kubectl delete pod $POD_NAME --ignore-not-found | Out-Null
    exit 1
}
Write-Host "[OK]   진단 완료" -ForegroundColor Green

# 6. kubectl cp 로 HTML 파일을 로컬로 복사 (UTF-8 바이너리 그대로 전송 → 한글 깨짐 없음)
# ※ kubectl cp 는 Windows 절대경로(C:\...)의 콜론을 remote 경로로 오인함
#   → 스크립트 디렉토리에서 파일명(콜론 없음)으로 복사 후 $Out 으로 이동
Write-Host "[INFO] 리포트 추출 중 (kubectl cp)..." -ForegroundColor Cyan
$tmpName = "aks-tmp-$POD_NAME.html"
$tmpFull = Join-Path $PSScriptRoot $tmpName
Push-Location $PSScriptRoot
$ErrorActionPreference = "Continue"
$cpOutput = kubectl cp "default/${POD_NAME}:/tmp/report.html" $tmpName 2>&1
$cpExit = $LASTEXITCODE
$ErrorActionPreference = "Stop"
Pop-Location
if ($cpExit -ne 0 -or -not (Test-Path $tmpFull)) {
    Write-Host "[FAIL] kubectl cp 실패 (exit=$cpExit): $cpOutput" -ForegroundColor Red
    Remove-Item $tmpFull -ErrorAction SilentlyContinue
    kubectl delete pod $POD_NAME --ignore-not-found | Out-Null
    exit 1
}
Move-Item $tmpFull $Out -Force
Write-Host "[OK]   Pod 정리 중..." -ForegroundColor Green
kubectl delete pod $POD_NAME --ignore-not-found | Out-Null

# 7. 결과 열기
if (Test-Path $Out) {
    $size = [math]::Round((Get-Item $Out).Length / 1KB, 1)
    Write-Host ""
    Write-Host "[OK] 완료! Report: $Out ($size KB)" -ForegroundColor Green
    Invoke-Item $Out
} else {
    Write-Host "[FAIL] 파일 복사 실패." -ForegroundColor Red
    exit 1
}
