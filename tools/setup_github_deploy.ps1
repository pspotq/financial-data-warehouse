# One-time setup for push-to-deploy (b245, 2026-10-27): lets GitHub Actions on pspotq/rrp-technologies (main branch only)
# deploy into rrp-technologies as the service account github-deployer, through Workload Identity Federation - GitHub
# proves who it is to Google on each run, so no key is ever created or stored. Safe to re-run (existing pieces are kept).
# Run from C:\dev\rrp in PowerShell:  .\tools\setup_github_deploy.ps1
$P = "rrp-technologies"; $PN = "1070977791744"; $REPO = "pspotq/rrp-technologies"
$SA = "github-deployer@$P.iam.gserviceaccount.com"; $RUNTIME = "rrp-ingest@$P.iam.gserviceaccount.com"
$ErrorActionPreference = "Continue"

gcloud config set project $P
gcloud services enable iamcredentials.googleapis.com sts.googleapis.com iam.googleapis.com cloudbuild.googleapis.com run.googleapis.com artifactregistry.googleapis.com

# 1. the deployer service account
if (-not (gcloud iam service-accounts list --filter="email:$SA" --format="value(email)")) {
  gcloud iam service-accounts create github-deployer --display-name "GitHub Actions deployer (rrp-technologies main)"
}

# 2. what it may do: deploy the two Cloud Run jobs from source (Cloud Build, its source bucket, the image registry),
#    run them as rrp-ingest, rebuild the warehouse views, and read the build logs. It cannot delete tables' data:
#    bigquery.dataEditor can, but dbt only ever replaces views.
foreach ($role in "roles/run.developer", "roles/cloudbuild.builds.editor", "roles/storage.admin", "roles/artifactregistry.writer",
                  "roles/serviceusage.serviceUsageConsumer", "roles/bigquery.dataEditor", "roles/bigquery.jobUser",
                  "roles/secretmanager.viewer", "roles/viewer") {
  gcloud projects add-iam-policy-binding $P --member "serviceAccount:$SA" --role $role --condition=None --quiet | Out-Null
  Write-Host "  granted $role"
}
gcloud iam service-accounts add-iam-policy-binding $RUNTIME --member "serviceAccount:$SA" --role roles/iam.serviceAccountUser --quiet | Out-Null

# 3. the trust link: a pool + a GitHub provider that only accepts this repo's main branch
if (-not (gcloud iam workload-identity-pools list --location global --format="value(name)" | Select-String "/github$")) {
  gcloud iam workload-identity-pools create github --location global --display-name "GitHub Actions"
}
if (-not (gcloud iam workload-identity-pools providers list --workload-identity-pool github --location global --format="value(name)" | Select-String "github-repo")) {
  gcloud iam workload-identity-pools providers create-oidc github-repo --location global --workload-identity-pool github `
    --display-name "pspotq/rrp-technologies" --issuer-uri "https://token.actions.githubusercontent.com" `
    --attribute-mapping "google.subject=assertion.sub,attribute.repository=assertion.repository,attribute.ref=assertion.ref" `
    --attribute-condition "assertion.repository == '$REPO' && assertion.ref == 'refs/heads/main'"
}
gcloud iam service-accounts add-iam-policy-binding $SA --role roles/iam.workloadIdentityUser `
  --member "principalSet://iam.googleapis.com/projects/$PN/locations/global/workloadIdentityPools/github/attribute.repository/$REPO" --quiet | Out-Null

Write-Host "Done. GitHub Actions on $REPO (main) can now deploy as $SA." -ForegroundColor Green
