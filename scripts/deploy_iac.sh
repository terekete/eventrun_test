#!/bin/sh
set -e

# export PULUMI_CONFIG_PASSPHRASE=test
# pulumi login gs://eventrun-state

cat DIFF_TEAM.txt | while read team
do
    gcloud builds submit . \
    --config=deploy.yaml \
    --project=eventrun \
    --impersonate-service-account=${team}-service-account@eventrun.iam.gserviceaccount.com \
    --substitutions=_ENV=$ENV,_ARTIFACT_BUCKET=$ARTIFACT_BUCKET,_TEAM=$team
    # python /workspace/scripts/iac.py $team
done
