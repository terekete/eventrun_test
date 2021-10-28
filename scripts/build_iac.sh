#!/bin/sh
set -e

ls -la
cat DIFF_TEAM.txt | while read team
do
    gcloud builds submit . --config=deploy.yaml --access-token-file=/workspace/${team} --project=eventrun --verbosity=error --account=${team}-service-account@eventrun.iam.gserviceaccount.com --impersonate-service-account=${team}-service-account@eventrun.iam.gserviceaccount.com --gcs-log-dir=gs://eventrun-logs --substitutions=_ENV=$ENV,_ARTIFACT_BUCKET=$ARTIFACT_BUCKET,_TEAM=$team,_STAGE_PROJECT=$STAGE_PROJECT
done
