#!/bin/sh

set -e


ls -la
gcloud auth list

# export PULUMI_CONFIG_PASSPHRASE=test
# pulumi login gs://eventrun-state


# cat DIFF_TEAM.txt | while read team
# do
#     python /workspace/scripts/iac.py $team
# done

