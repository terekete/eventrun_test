
#!/bin/sh

set -e

export PULUMI_CONFIG_PASSPHRASE=test
pulumi login gs://eventrun-state
pulumi plugin install resource gcp v5.23.0
# python /workspace/scripts/sa.py

cat DIFF_TEAM.txt | while read team
do
    echo "team:" ${team}
    python /workspace/scripts/sa.py $team
    export GOOGLE_APPLICATION_CREDENTIALS="/workspace/${team}.json"
    python /workspace/scripts/iac.py $team
done
ls -la
pwd
