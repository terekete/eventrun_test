
#!/bin/sh

set -e
export PULUMI_CONFIG_PASSPHRASE=test
pulumi login gs://eventrun-state

cat DIFF_TEAM.txt | while read team
do
    python /workspace/scripts/sa.py $team
done


