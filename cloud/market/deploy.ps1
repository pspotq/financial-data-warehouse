# RRP market-data job: build, deploy and schedule on Google Cloud (project rrp-technologies).
# Run from C:\dev\rrp\cloud\market in PowerShell. Safe to re-run: existing pieces are updated, not duplicated.
# -NoRun skips the test execution at the end.
param([switch]$NoRun)
$P = "rrp-technologies"; $R = "us-central1"; $JOB = "rrp-market"
$SA = "rrp-ingest@$P.iam.gserviceaccount.com"

gcloud config set project $P
gcloud services enable run.googleapis.com cloudbuild.googleapis.com artifactregistry.googleapis.com cloudscheduler.googleapis.com bigquery.googleapis.com secretmanager.googleapis.com

# service account the job and the scheduler run as
if (-not (gcloud iam service-accounts list --filter="email:$SA" --format="value(email)")) {
  gcloud iam service-accounts create rrp-ingest --display-name "RRP ingest jobs"
}
foreach ($role in "roles/bigquery.dataEditor", "roles/bigquery.jobUser", "roles/run.invoker") {
  gcloud projects add-iam-policy-binding $P --member "serviceAccount:$SA" --role $role --condition=None --quiet | Out-Null
}

# API keys from Secret Manager (added 2026-10-23): eia-api-key -> EIA_API_KEY, mounted only once the secret exists
$SECRETS = @()
if (gcloud secrets list --project $P --filter="name~eia-api-key" --format="value(name)") {
  gcloud secrets add-iam-policy-binding eia-api-key --project $P --member "serviceAccount:$SA" --role roles/secretmanager.secretAccessor --quiet | Out-Null
  $SECRETS = @("--set-secrets", "EIA_API_KEY=eia-api-key:latest")
}

# build from this folder (Dockerfile) and deploy the job
gcloud run jobs deploy $JOB --source . --region $R --service-account $SA `
  --tasks 1 --max-retries 1 --task-timeout 600 --memory 512Mi --cpu 1 `
  --set-env-vars RRP_PROJECT=$P @SECRETS
if ($LASTEXITCODE -ne 0) { Write-Host "Deploy failed - stopping before the schedule and test run." -ForegroundColor Red; exit 1 }

# the two daily schedules start the job with different arguments and a longer timeout. Starting a job with overrides
# needs more than run.invoker, so the scheduler's service account gets run.developer on THIS job only (added 2026-10-01
# after the first 22:20 UTC stock pass never started).
gcloud run jobs add-iam-policy-binding $JOB --region $R --member "serviceAccount:$SA" --role roles/run.developer --quiet | Out-Null

# every 30 minutes (was 15 until 2026-10-21; scheduler keeps its old name rrp-market-15m)
$URI = "https://run.googleapis.com/v2/projects/$P/locations/$R/jobs/${JOB}:run"
if (gcloud scheduler jobs list --location $R --format="value(name)" | Select-String "$JOB-15m") {
  gcloud scheduler jobs update http "$JOB-15m" --location $R --schedule "*/30 * * * *" --uri $URI --http-method POST --oauth-service-account-email $SA
} else {
  gcloud scheduler jobs create http "$JOB-15m" --location $R --schedule "*/30 * * * *" --time-zone "Etc/UTC" --uri $URI --http-method POST --oauth-service-account-email $SA
}

# daily stocks/ETFs pass (build 0.2): its own execution with a 1-hour timeout, 22:20 UTC Mon-Fri (after the US close)
$EQ = "$JOB-equities"
if (gcloud scheduler jobs list --location $R --format="value(name)" | Select-String $EQ) {
  gcloud scheduler jobs update http $EQ --location $R --schedule "20 22 * * 1-5" --uri $URI --http-method POST --oauth-service-account-email $SA --message-body-from-file equities_run.json --update-headers "Content-Type=application/json"
} else {
  gcloud scheduler jobs create http $EQ --location $R --schedule "20 22 * * 1-5" --time-zone "Etc/UTC" --uri $URI --http-method POST --oauth-service-account-email $SA --message-body-from-file equities_run.json --headers "Content-Type=application/json"
}

# daily crypto candles (build 0.3): every Kraken + Coinbase USD pair, last 5 days, 00:15 UTC every day
$CD = "$JOB-crypto-daily"
if (gcloud scheduler jobs list --location $R --format="value(name)" | Select-String $CD) {
  gcloud scheduler jobs update http $CD --location $R --schedule "15 0 * * *" --uri $URI --http-method POST --oauth-service-account-email $SA --message-body-from-file crypto_daily_run.json --update-headers "Content-Type=application/json"
} else {
  gcloud scheduler jobs create http $CD --location $R --schedule "15 0 * * *" --time-zone "Etc/UTC" --uri $URI --http-method POST --oauth-service-account-email $SA --message-body-from-file crypto_daily_run.json --headers "Content-Type=application/json"
}

# one run now to prove it end to end
if (-not $NoRun) { gcloud run jobs execute $JOB --region $R --wait }
