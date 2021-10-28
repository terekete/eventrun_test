#!/bin/sh
set -e

# export PULUMI_CONFIG_PASSPHRASE=test
# pulumi login gs://eventrun-state

ls -la
cat DIFF_TEAM.txt | while read team
do
    gcloud builds submit . --config=deploy.yaml --project=eventrun --verbosity=error --impersonate-service-account=${team}-service-account@eventrun.iam.gserviceaccount.com --gcs-log-dir=gs://eventrun-logs --substitutions=_ENV=$ENV,_ARTIFACT_BUCKET=$ARTIFACT_BUCKET,_TEAM=$team,_STAGE_PROJECT=$STAGE_PROJECT
    # python /workspace/scripts/iac.py $team
done