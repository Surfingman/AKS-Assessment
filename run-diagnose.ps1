# run-diagnose.ps1
# AKS 진단 도구 실행 스크립트

param(
    [string]$Namespace = "",
    [string]$PrometheusUrl = "http://prom-prometheus-server.monitoring.svc.cluster.local:80",
    [string]$Image = "factorykr.azurecr.io/aks-diagnose:latest",
    [switch]$LocalImage,
    [switch]$UseAzureManagedPrometheus,
    [string]$ManagedIdentityName = "aks-diagnose-mi",
    [string]$AzureMonitorWorkspaceName = "",
    [string]$Out = ""
)

if ($Out -eq "") {
    $ts = Get-Date -Format "yyyyMMdd_HHmm"
    $Out = "$PSScriptRoot\report_$ts.html"
}

$ErrorActionPreference = "Stop"

$SCRIPT_DIR = $PSScriptRoot
$POD_NAMESPACE = "default"
$SERVICE_ACCOUNT = "aks-diagnose"
$POD_NAME = "aks-diagnose-$(Get-Date -Format 'HHmmss')"
$PrometheusAad = $false

function Require-Command($Name, $Hint) {
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        Write-Host "[FAIL] $Name not found. $Hint" -ForegroundColor Red
        exit 1
    }
}

function Get-CurrentAksFromKubectlContext {
    $server = kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}'
    if (-not $server) {
        throw "kubectl context에서 cluster.server를 읽지 못했습니다."
    }

    $aksList = az aks list -o json | ConvertFrom-Json
    $match = $aksList | Where-Object {
        ($_.fqdn -and $server.Contains($_.fqdn)) -or
        ($_.privateFqdn -and $server.Contains($_.privateFqdn))
    } | Select-Object -First 1

    if (-not $match) {
        throw "현재 kubectl context와 일치하는 AKS를 찾지 못했습니다."
    }

    return $match
}

function Ensure-AzureManagedPrometheus {
    Write-Host "[INFO] Azure Managed Prometheus 자동 설정..." -ForegroundColor Cyan

    $aks = Get-CurrentAksFromKubectlContext
    $ResourceGroup = $aks.resourceGroup
    $AksName = $aks.name

    Write-Host "[INFO] AKS detected: $ResourceGroup / $AksName" -ForegroundColor Cyan

    $aksInfo = az aks show `
        --resource-group $ResourceGroup `
        --name $AksName `
        -o json | ConvertFrom-Json

    $issuer = $aksInfo.oidcIssuerProfile.issuerUrl
    $wiEnabled = $false

    if ($aksInfo.securityProfile -and $aksInfo.securityProfile.workloadIdentity) {
        $wiEnabled = [bool]$aksInfo.securityProfile.workloadIdentity.enabled
    }

    if (-not $issuer -or -not $wiEnabled) {
        Write-Host "[INFO] OIDC / Workload Identity 활성화 중..." -ForegroundColor Cyan

        az aks update `
            --resource-group $ResourceGroup `
            --name $AksName `
            --enable-oidc-issuer `
            --enable-workload-identity | Out-Null

        $aksInfo = az aks show `
            --resource-group $ResourceGroup `
            --name $AksName `
            -o json | ConvertFrom-Json

        $issuer = $aksInfo.oidcIssuerProfile.issuerUrl
    }

    if (-not $issuer) {
        throw "OIDC issuer URL을 가져오지 못했습니다."
    }

    if ($AzureMonitorWorkspaceName -eq "") {
        $workspaces = az monitor account list `
            --resource-group $ResourceGroup `
            -o json | ConvertFrom-Json

        if (-not $workspaces -or $workspaces.Count -eq 0) {
            throw "Azure Monitor Workspace를 찾지 못했습니다."
        }

        $workspace = $workspaces | Select-Object -First 1
    } else {
        $workspace = az monitor account show `
            --resource-group $ResourceGroup `
            --name $AzureMonitorWorkspaceName `
            -o json | ConvertFrom-Json
    }

    $WorkspaceId = $workspace.id
    $PromUrl = $workspace.metrics.prometheusQueryEndpoint

    if (-not $PromUrl) {
        throw "prometheusQueryEndpoint를 찾지 못했습니다."
    }

    Write-Host "[INFO] Azure Monitor Workspace: $($workspace.name)" -ForegroundColor Cyan
    Write-Host "[INFO] Prometheus endpoint: $PromUrl" -ForegroundColor Cyan

    $miJson = $null
    $ErrorActionPreference = "Continue"
    $miRaw = az identity show `
        --resource-group $ResourceGroup `
        --name $ManagedIdentityName `
        -o json 2>$null
    $miExit = $LASTEXITCODE
    $ErrorActionPreference = "Stop"

    if ($miExit -ne 0 -or -not $miRaw) {
        Write-Host "[INFO] Managed Identity 생성: $ManagedIdentityName" -ForegroundColor Cyan

        az identity create `
            --resource-group $ResourceGroup `
            --name $ManagedIdentityName | Out-Null
    } else {
        Write-Host "[OK]   Managed Identity exists: $ManagedIdentityName" -ForegroundColor Green
    }

    $miJson = az identity show `
        --resource-group $ResourceGroup `
        --name $ManagedIdentityName `
        -o json | ConvertFrom-Json

    $ClientId = $miJson.clientId
    $PrincipalId = $miJson.principalId

    Write-Host "[INFO] Managed Identity clientId: $ClientId" -ForegroundColor Cyan

    $roleCount = az role assignment list `
        --assignee-object-id $PrincipalId `
        --scope $WorkspaceId `
        --query "[?roleDefinitionName=='Monitoring Data Reader'] | length(@)" `
        -o tsv

    if ([int]$roleCount -eq 0) {
        Write-Host "[INFO] Monitoring Data Reader 권한 부여..." -ForegroundColor Cyan

        az role assignment create `
            --assignee-object-id $PrincipalId `
            --assignee-principal-type ServicePrincipal `
            --role "Monitoring Data Reader" `
            --scope $WorkspaceId | Out-Null
    } else {
        Write-Host "[OK]   Monitoring Data Reader already assigned" -ForegroundColor Green
    }

    $subject = "system:serviceaccount:${POD_NAMESPACE}:${SERVICE_ACCOUNT}"

    $fedList = az identity federated-credential list `
        --resource-group $ResourceGroup `
        --identity-name $ManagedIdentityName `
        -o json | ConvertFrom-Json

    $fed = $fedList | Where-Object {
        $_.issuer -eq $issuer -and $_.subject -eq $subject
    } | Select-Object -First 1

    if (-not $fed) {
        Write-Host "[INFO] Federated Credential 생성..." -ForegroundColor Cyan

        az identity federated-credential create `
            --resource-group $ResourceGroup `
            --identity-name $ManagedIdentityName `
            --name "aks-diagnose-fed" `
            --issuer $issuer `
            --subject $subject | Out-Null
    } else {
        Write-Host "[OK]   Federated Credential already exists" -ForegroundColor Green
    }

    Write-Host "[INFO] ServiceAccount annotation 업데이트..." -ForegroundColor Cyan

    kubectl annotate sa $SERVICE_ACCOUNT -n $POD_NAMESPACE `
        azure.workload.identity/client-id="$ClientId" `
        --overwrite | Out-Null

    return @{
        PrometheusUrl = $PromUrl
        ClientId = $ClientId
    }
}

Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  AKS Diagnose Tool" -ForegroundColor Cyan
Write-Host "  Image : $Image" -ForegroundColor Cyan
Write-Host "  Output: $Out" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan

Require-Command "kubectl" "Install: winget install Kubernetes.kubectl"

if ($UseAzureManagedPrometheus) {
    Require-Command "az" "Install: winget install Microsoft.AzureCLI"
}

Write-Host "[INFO] Context: $(kubectl config current-context)" -ForegroundColor Cyan

Write-Host "[INFO] Applying RBAC..." -ForegroundColor Cyan
kubectl apply -f "$SCRIPT_DIR\k8s\rbac.yaml" | Out-Null
Write-Host "[OK]   RBAC ready" -ForegroundColor Green

if ($UseAzureManagedPrometheus) {
    $managed = Ensure-AzureManagedPrometheus
    $PrometheusUrl = $managed.PrometheusUrl
    $PrometheusAad = $true
}

$pyArgs = @(
    "--in-cluster",
    "--prometheus-url", $PrometheusUrl,
    "--out", "/tmp/report.html",
    "--no-history"
)

if ($PrometheusAad) {
    $pyArgs += "--prometheus-aad"
}

if ($Namespace -ne "") {
    $pyArgs = @("-n", $Namespace) + $pyArgs
    Write-Host "[INFO] Target: namespace=$Namespace" -ForegroundColor Cyan
} else {
    $pyArgs = @("-A") + $pyArgs
    Write-Host "[INFO] Target: all namespaces (-A)" -ForegroundColor Cyan
}

Write-Host "[INFO] PrometheusUrl: $PrometheusUrl" -ForegroundColor Cyan
Write-Host "[INFO] PrometheusAad: $PrometheusAad" -ForegroundColor Cyan

$fullCmd = "python aks_diagnose.py $(($pyArgs | ForEach-Object { $_ }) -join ' ') && echo __DIAG_DONE__ || echo __DIAG_FAILED__; sleep 120"
$pullPolicy = if ($LocalImage) { "Never" } else { "Always" }

$yaml = @()
$yaml += "apiVersion: v1"
$yaml += "kind: Pod"
$yaml += "metadata:"
$yaml += "  name: $POD_NAME"
$yaml += "  namespace: $POD_NAMESPACE"

if ($UseAzureManagedPrometheus) {
    $yaml += "  labels:"
    $yaml += "    azure.workload.identity/use: `"true`""
}

$yaml += "spec:"
$yaml += "  serviceAccountName: $SERVICE_ACCOUNT"
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

Write-Host "[INFO] Running pod: $POD_NAME ..." -ForegroundColor Cyan
kubectl apply -f $tmpYaml | Out-Null
Remove-Item $tmpYaml -ErrorAction SilentlyContinue
Write-Host "[OK]   Pod created" -ForegroundColor Green

Write-Host "[INFO] 진단 완료 대기 중..." -ForegroundColor Cyan

$timeout = 240
$elapsed = 0
$done = $false

while ($elapsed -lt $timeout) {
    $ErrorActionPreference = "Continue"
    $phase = kubectl get pod $POD_NAME -n $POD_NAMESPACE -o jsonpath='{.status.phase}' 2>$null
    $ErrorActionPreference = "Stop"

    if ($phase -eq "Failed") {
        Write-Host "[FAIL] Pod 실패!" -ForegroundColor Red
        kubectl describe pod $POD_NAME -n $POD_NAMESPACE
        kubectl logs $POD_NAME -n $POD_NAMESPACE
        kubectl delete pod $POD_NAME -n $POD_NAMESPACE --ignore-not-found | Out-Null
        exit 1
    }

    if ($phase -ne "Running") {
        Start-Sleep -Seconds 5
        $elapsed += 5
        Write-Host "  Pod 시작 대기 중... ($elapsed / $timeout 초) [phase: $phase]" -ForegroundColor Gray
        continue
    }

    $ErrorActionPreference = "Continue"
    $logs = kubectl logs $POD_NAME -n $POD_NAMESPACE 2>$null
    $ErrorActionPreference = "Stop"

    if ($logs -match "__DIAG_FAILED__") {
        Write-Host "[FAIL] 진단 스크립트 오류. Pod 로그:" -ForegroundColor Red
        Write-Host $logs -ForegroundColor Yellow
        Write-Host "kubectl exec -it $POD_NAME -n $POD_NAMESPACE -- sh" -ForegroundColor Gray
        kubectl delete pod $POD_NAME -n $POD_NAMESPACE --ignore-not-found | Out-Null
        exit 1
    }

    if ($logs -match "__DIAG_DONE__") {
        $done = $true
        break
    }

    Start-Sleep -Seconds 5
    $elapsed += 5
    Write-Host "  대기 중... ($elapsed / $timeout 초) [phase: $phase]" -ForegroundColor Gray
}

if (-not $done) {
    Write-Host "[FAIL] 타임아웃 ($timeout 초). Pod 로그:" -ForegroundColor Red
    kubectl logs $POD_NAME -n $POD_NAMESPACE
    kubectl delete pod $POD_NAME -n $POD_NAMESPACE --ignore-not-found | Out-Null
    exit 1
}

Write-Host "[OK]   진단 완료" -ForegroundColor Green

Write-Host "[INFO] 리포트 추출 중 (kubectl cp)..." -ForegroundColor Cyan

$tmpName = "aks-tmp-$POD_NAME.html"
$tmpFull = Join-Path $PSScriptRoot $tmpName

Push-Location $PSScriptRoot
$ErrorActionPreference = "Continue"
$cpOutput = kubectl cp "${POD_NAMESPACE}/${POD_NAME}:/tmp/report.html" $tmpName 2>&1
$cpExit = $LASTEXITCODE
$ErrorActionPreference = "Stop"
Pop-Location

if ($cpExit -ne 0 -or -not (Test-Path $tmpFull)) {
    Write-Host "[FAIL] kubectl cp 실패 (exit=$cpExit): $cpOutput" -ForegroundColor Red
    Remove-Item $tmpFull -ErrorAction SilentlyContinue
    kubectl delete pod $POD_NAME -n $POD_NAMESPACE --ignore-not-found | Out-Null
    exit 1
}

Move-Item $tmpFull $Out -Force

Write-Host "[OK]   Pod 정리 중..." -ForegroundColor Green
kubectl delete pod $POD_NAME -n $POD_NAMESPACE --ignore-not-found | Out-Null

if (Test-Path $Out) {
    $size = [math]::Round((Get-Item $Out).Length / 1KB, 1)
    Write-Host ""
    Write-Host "[OK] 완료! Report: $Out ($size KB)" -ForegroundColor Green
    Invoke-Item $Out
} else {
    Write-Host "[FAIL] 파일 복사 실패." -ForegroundColor Red
    exit 1
}