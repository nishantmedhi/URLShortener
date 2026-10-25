$ErrorActionPreference = "Stop"

$overlays = @(
    "k8s/overlays/local",
    "k8s/overlays/canary",
    "k8s/overlays/dedicated",
    "k8s/overlays/production",
    "k8s/overlays/production-hardening"
)

foreach ($overlay in $overlays) {
    Write-Host "Rendering $overlay"
    kubectl kustomize $overlay | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "Kustomize render failed: $overlay"
    }
}

$dbSchema = Get-Content -Raw db/init.sql
$k8sSchema = Get-Content -Raw k8s/local-dependencies/postgres-init.sql
if ($dbSchema -ne $k8sSchema) {
    throw "Kubernetes local PostgreSQL schema is out of sync with db/init.sql"
}

Write-Host "All Kubernetes overlays rendered and the local DB schema matches."
