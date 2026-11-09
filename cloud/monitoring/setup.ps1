# RRP phone alerts (Cloud Monitoring), added 2026-10-19.
# Run once from C:\dev\rrp\cloud\monitoring after the Google Cloud app on your phone has opened project rrp-technologies:
#   .\setup.ps1 -Channel projects/rrp-technologies/notificationChannels/<id>
# Creates: 2 log-based metrics (rrp_alert_red, rrp_heartbeat), 3 alert policies (something RED, health check stopped,
# a Cloud Run job failed), and a $10/month budget that pushes to the same phone at 50%, 90% and 100%.
# Safe to re-run: existing metrics are updated, and policies are skipped when one with the same name exists.
param([Parameter(Mandatory = $true)][string]$Channel, [int]$BudgetUsd = 10)
$ErrorActionPreference = "Stop"
$P = "rrp-technologies"
Set-Location $PSScriptRoot

Write-Host "== log-based metrics"
foreach ($m in @("rrp_alert_red", "rrp_heartbeat")) {
    $file = if ($m -eq "rrp_alert_red") { "metric_red.yaml" } else { "metric_heartbeat.yaml" }
    $exists = gcloud logging metrics list --project $P --format="value(name)" --filter="name=$m"
    if ($exists) { gcloud logging metrics update $m --project $P --config-from-file=$file }
    else { gcloud logging metrics create $m --project $P --config-from-file=$file }
}

Write-Host "== alert policies"
$have = gcloud alpha monitoring policies list --project $P --format="value(displayName)"
foreach ($f in @("policy_red.json", "policy_heartbeat.json", "policy_job_failed.json")) {
    $json = (Get-Content $f -Raw).Replace('"CHANNEL"', '"' + $Channel + '"')
    $name = ($json | ConvertFrom-Json).displayName
    if ($have -contains $name) { Write-Host "  exists: $name"; continue }
    $tmp = Join-Path $env:TEMP "rrp_$f"
    Set-Content -Path $tmp -Value $json -Encoding ascii
    gcloud alpha monitoring policies create --project $P --policy-from-file=$tmp
    Write-Host "  created: $name"
}

Write-Host "== budget"
$acct = (gcloud billing projects describe $P --format="value(billingAccountName)").Replace("billingAccounts/", "")
$budgets = gcloud billing budgets list --billing-account=$acct --format="value(displayName)"
if ($budgets -contains "RRP monthly") { Write-Host "  exists: RRP monthly" }
else {
    gcloud billing budgets create --billing-account=$acct --display-name="RRP monthly" `
        --budget-amount="$($BudgetUsd)USD" --filter-projects="projects/$P" `
        --threshold-rule=percent=0.5 --threshold-rule=percent=0.9 --threshold-rule=percent=1.0 `
        --notifications-rule-monitoring-notification-channels=$Channel
    Write-Host "  created: RRP monthly ($BudgetUsd USD)"
}
Write-Host "done"
