# RRP feeds job (SEC filings, Form 4s, filing text, CFTC, DoD): build, deploy and schedule hourly at :05.
# Run from C:\dev\rrp\cloud\feeds. Safe to re-run. -NoRun skips the test execution at the end.
param([switch]$NoRun)
$P = "rrp-technologies"; $R = "us-central1"; $JOB = "rrp-feeds"
$SA = "rrp-ingest@$P.iam.gserviceaccount.com"

gcloud config set project $P
gcloud run jobs deploy $JOB --source . --region $R --service-account $SA `
  --tasks 1 --max-retries 1 --task-timeout 3600 --memory 1Gi --cpu 1 `
  --set-env-vars "RRP_PROJECT=$P"   # replaces every env var (the old FORM4 switch goes)
if ($LASTEXITCODE -ne 0) { Write-Host "Deploy failed - stopping before the schedule and test run." -ForegroundColor Red; exit 1 }

$URI = "https://run.googleapis.com/v2/projects/$P/locations/$R/jobs/${JOB}:run"
if (gcloud scheduler jobs list --location $R --format="value(name)" | Select-String "$JOB-hourly") {
  gcloud scheduler jobs update http "$JOB-hourly" --location $R --schedule "5 * * * *" --uri $URI --http-method POST --oauth-service-account-email $SA
} else {
  gcloud scheduler jobs create http "$JOB-hourly" --location $R --schedule "5 * * * *" --time-zone "Etc/UTC" --uri $URI --http-method POST --oauth-service-account-email $SA
}
if (-not $NoRun) { gcloud run jobs execute $JOB --region $R --wait }
