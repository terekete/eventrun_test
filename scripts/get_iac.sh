#! /bin/bash

export PULUMI_CONFIG_PASSPHRASE=test

pulumi login gs://eventrun-state
pulumi stack select dev -c --secrets-provider=test
pulumi config set gcp:project eventrun
pulumi config set gcp:region northamerica-northeast1
pulumi version
pulumi refresh --yes --skip-preview --suppress-outputs
pulumi up --yes --skip-preview --suppress-outputs
