param(
    [ValidateSet("bluegreen", "canary")]
    [string]$Strategy = "bluegreen",
    [switch]$ApproveInitialPromotion,
    [switch]$TestRollback,
    [string]$Namespace = "url-shortener"
)

$ErrorActionPreference = "Stop"
if ($TestRollback -and $Strategy -ne "bluegreen") {
    throw "-TestRollback currently exercises the blue/green active-service rollback; canary rollback remains available through the Argo Rollouts CLI."
}
$rolloutsVersion = "v1.8.3"
$rolloutsUrl = "https://github.com/argoproj/argo-rollouts/releases/download/$rolloutsVersion/install.yaml"
$pluginUrl = "https://github.com/argoproj/argo-rollouts/releases/download/$rolloutsVersion/kubectl-argo-rollouts-windows-amd64"
$toolDir = Join-Path $env:TEMP "url-shortener-argo-rollouts"
$null = New-Item -ItemType Directory -Force -Path $toolDir
$pluginPath = Join-Path $toolDir "kubectl-argo-rollouts.exe"
$portForward = $null
$rolloutPatchFile = $null

function Invoke-Kubectl {
    param([Parameter(ValueFromRemainingArguments = $true)][string[]]$Arguments)
    & kubectl @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "kubectl failed: $($Arguments -join ' ')"
    }
}

function Wait-ForPromotionGate {
    param([string]$MinimumCanaryStep)
    for ($attempt = 0; $attempt -lt 90; $attempt++) {
        $json = & kubectl get rollout url-shortener --namespace $Namespace -o json
        if ($LASTEXITCODE -ne 0) {
            throw "Could not read Argo Rollout status."
        }
        $status = $json | ConvertFrom-Json
        if ($status.status.phase -eq "Degraded") {
            throw "Argo Rollout is degraded before promotion."
        }
        $atRequiredCanaryStep = $Strategy -ne "canary" `
            -or [int]$status.status.currentStepIndex -ge [int]$MinimumCanaryStep
        if ($status.status.phase -eq "Paused" -and $atRequiredCanaryStep) {
            return
        }
        Start-Sleep -Seconds 5
    }
    throw "Argo Rollout did not reach its human promotion gate in time."
}

try {
    Invoke-Kubectl cluster-info --request-timeout=10s | Out-Null
    if (-not (Test-Path $pluginPath)) {
        Write-Host "Installing the pinned Argo Rollouts $rolloutsVersion kubectl plugin."
        Invoke-WebRequest -Uri $pluginUrl -OutFile $pluginPath
    }
    $env:PATH = "$toolDir;$env:PATH"

    $controllerDeploymentsJson = & kubectl get deployments --all-namespaces --output json
    if ($LASTEXITCODE -ne 0) {
        throw "Could not check for an existing Argo Rollouts controller."
    }
    $controllerDeployments = @(
        ($controllerDeploymentsJson | ConvertFrom-Json).items |
            Where-Object { $_.metadata.name -eq "argo-rollouts" }
    )
    if ($controllerDeployments.Count -gt 1) {
        throw "Multiple Argo Rollouts controllers were found; refusing to deploy an ambiguous controller."
    }
    if ($controllerDeployments.Count -eq 0) {
        $controllerNamespace = Invoke-Kubectl get namespace argo-rollouts --ignore-not-found --output name
        if (-not $controllerNamespace) {
            Invoke-Kubectl create namespace argo-rollouts | Out-Null
        }
        & kubectl apply --namespace=argo-rollouts --filename $rolloutsUrl
        if ($LASTEXITCODE -ne 0) {
            throw "Could not install the pinned Argo Rollouts controller."
        }
        $controllerNamespace = "argo-rollouts"
    } else {
        $controllerNamespace = $controllerDeployments[0].metadata.namespace
    }
    Invoke-Kubectl rollout status deployment/argo-rollouts --namespace $controllerNamespace --timeout=240s | Out-Null

    Write-Host "Building urlshortener:local for the local Kubernetes image cache."
    docker build --tag urlshortener:local .
    if ($LASTEXITCODE -ne 0) {
        throw "Docker image build failed."
    }

    $overlay = if ($Strategy -eq "canary") { "k8s/overlays/canary" } else { "k8s/overlays/local" }
    Invoke-Kubectl apply --kustomize $overlay
    Invoke-Kubectl rollout status statefulset/postgres --namespace $Namespace --timeout=240s | Out-Null
    Invoke-Kubectl rollout status statefulset/redpanda --namespace $Namespace --timeout=300s | Out-Null
    Invoke-Kubectl rollout status deployment/redis --namespace $Namespace --timeout=180s | Out-Null
    Invoke-Kubectl rollout status deployment/prometheus --namespace $Namespace --timeout=180s | Out-Null
    Invoke-Kubectl wait --for=condition=complete job/url-shortener-topic-init --namespace $Namespace --timeout=180s | Out-Null
    Invoke-Kubectl rollout status deployment/analytics-worker --namespace $Namespace --timeout=240s | Out-Null
    Invoke-Kubectl wait --for=condition=Ready pod --selector=app.kubernetes.io/name=url-shortener --namespace $Namespace --timeout=240s | Out-Null
    $initialRollout = (& kubectl get rollout url-shortener --namespace $Namespace -o json) | ConvertFrom-Json
    $initialHash = $initialRollout.status.currentPodHash
    $activeHash = (& kubectl get service url-shortener-active --namespace $Namespace `
        -o jsonpath='{.spec.selector.rollouts-pod-template-hash}').Trim()
    $initialPromotionRequired = -not (
        $initialRollout.status.phase -eq "Healthy" -and
        $initialHash -and
        $activeHash -eq $initialHash
    )
    if ($initialPromotionRequired) {
        Wait-ForPromotionGate -MinimumCanaryStep "4"
        if (-not $ApproveInitialPromotion) {
            $answer = Read-Host "Preview is ready. Promote the URL Shortener release? (y/N)"
            if ($answer -ne "y") {
                throw "Promotion was not approved; the preview remains inactive."
            }
        }
        Invoke-Kubectl argo rollouts promote url-shortener --namespace $Namespace
        Invoke-Kubectl argo rollouts status url-shortener --namespace $Namespace --timeout 240s
    } else {
        Write-Host "The initial release is already active; no first-deployment promotion gate is required."
    }

    $stdout = Join-Path $env:TEMP "url-shortener-port-forward.out.log"
    $stderr = Join-Path $env:TEMP "url-shortener-port-forward.err.log"
    $kubectlPath = (Get-Command kubectl).Source
    $portForward = Start-Process -FilePath $kubectlPath `
        -ArgumentList @("port-forward", "service/url-shortener-active", "18080:80", "--namespace", $Namespace) `
        -PassThru -WindowStyle Hidden -RedirectStandardOutput $stdout -RedirectStandardError $stderr

    $ready = $false
    for ($attempt = 0; $attempt -lt 30; $attempt++) {
        try {
            $response = Invoke-RestMethod -Uri "http://127.0.0.1:18080/health/ready" -TimeoutSec 2
            if ($response.status -eq "ready") {
                $ready = $true
                break
            }
        } catch {
            Start-Sleep -Seconds 2
        }
    }
    if (-not $ready) {
        throw "The active service did not become ready through port-forward."
    }

    $link = Invoke-RestMethod -Method Post `
        -Uri "http://127.0.0.1:18080/api/v1/links" `
        -ContentType "application/json" `
        -Body '{"url":"https://example.com/kubernetes-smoke"}'
    $redirectCode = & curl.exe --silent --show-error --output NUL --write-out "%{http_code}" `
        "http://127.0.0.1:18080/$($link.code)"
    if ($redirectCode -ne "302") {
        throw "Redirect smoke test returned HTTP $redirectCode instead of 302."
    }

    $analyticsReady = $false
    for ($attempt = 0; $attempt -lt 30; $attempt++) {
        $analytics = Invoke-RestMethod -Uri `
            "http://127.0.0.1:18080/api/v1/links/$($link.code)/analytics?days=1"
        if ($analytics.total_clicks -ge 1) {
            $analyticsReady = $true
            break
        }
        Start-Sleep -Seconds 2
    }
    if (-not $analyticsReady) {
        throw "Redirect succeeded but its durable click event was not visible in analytics."
    }
    Write-Host "Kubernetes API, redirect, durable analytics, probes, and rollout promotion passed."

    if ($TestRollback) {
        $previousHash = (& kubectl get rollout url-shortener --namespace $Namespace `
            -o jsonpath='{.status.currentPodHash}').Trim()
        $revision = [guid]::NewGuid().ToString("N")
        $patch = @{
            spec = @{
                template = @{
                    metadata = @{
                        annotations = @{ "prototype-revision" = $revision }
                    }
                }
            }
        } | ConvertTo-Json -Depth 8 -Compress
        $rolloutPatchFile = Join-Path $env:TEMP "url-shortener-rollout-$revision.json"
        Set-Content -LiteralPath $rolloutPatchFile -Value $patch -Encoding ASCII
        Invoke-Kubectl patch rollout url-shortener --namespace $Namespace `
            --type merge --patch-file $rolloutPatchFile | Out-Null
        Remove-Item -LiteralPath $rolloutPatchFile -Force
        $rolloutPatchFile = $null
        $newHash = $previousHash
        for ($attempt = 0; $attempt -lt 60 -and $newHash -eq $previousHash; $attempt++) {
            Start-Sleep -Seconds 2
            $newHash = (& kubectl get rollout url-shortener --namespace $Namespace `
                -o jsonpath='{.status.currentPodHash}').Trim()
        }
        if (-not $newHash -or $newHash -eq $previousHash) {
            throw "Rollout controller did not create a new ReplicaSet."
        }
        Invoke-Kubectl wait --for=condition=Ready pod `
            --selector="app.kubernetes.io/name=url-shortener,rollouts-pod-template-hash=$newHash" `
            --namespace $Namespace --timeout=240s | Out-Null
        Wait-ForPromotionGate -MinimumCanaryStep "4"
        $answer = Read-Host "Rollback probe is ready. Approve this revision before exercising rollback? (y/N)"
        if ($answer -ne "y") {
            Invoke-Kubectl argo rollouts abort url-shortener --namespace $Namespace | Out-Null
            throw "The revision was not approved; rollout was aborted and the old stable service remains active."
        }
        Invoke-Kubectl argo rollouts promote url-shortener --namespace $Namespace
        Invoke-Kubectl argo rollouts status url-shortener --namespace $Namespace --timeout 240s
        Invoke-Kubectl argo rollouts undo url-shortener --namespace $Namespace
        Wait-ForPromotionGate -MinimumCanaryStep "4"
        $answer = Read-Host "Rollback is paused for approval. Promote the restored revision? (y/N)"
        if ($answer -ne "y") {
            Invoke-Kubectl argo rollouts abort url-shortener --namespace $Namespace | Out-Null
            throw "Rollback was not approved; the currently active revision remains in service."
        }
        Invoke-Kubectl argo rollouts promote url-shortener --namespace $Namespace
        Invoke-Kubectl argo rollouts status url-shortener --namespace $Namespace --timeout 240s
        $afterRollback = Invoke-RestMethod -Uri "http://127.0.0.1:18080/health/ready" -TimeoutSec 5
        if ($afterRollback.status -ne "ready") {
            throw "Active service did not recover after rollout undo."
        }
        Write-Host "Argo Rollouts promotion and rollback probe passed."
    }
} catch {
    Write-Error "Kubernetes end-to-end prototype failed: $_"
    exit 1
} finally {
    if ($portForward -and -not $portForward.HasExited) {
        Stop-Process -Id $portForward.Id -Force
    }
    if ($rolloutPatchFile -and (Test-Path -LiteralPath $rolloutPatchFile)) {
        Remove-Item -LiteralPath $rolloutPatchFile -Force
    }
}
