
#!/bin/sh

set -e

export PULUMI_CONFIG_PASSPHRASE=test
pulumi login gs://eventrun-state
pulumi plugin install resource gcp v5.24.0



cat DIFF_TEAM.txt | while read team
do
    python /workspace/scripts/sa.py $team
done
cat DIFF_TEAM.txt | while read team
do
    export GOOGLE_APPLICATION_CREDENTIALS="/workspace/${team}.json"
    python /workspace/scripts/iac.py $team
done
