
#!/bin/sh

export PULUMI_CONFIG_PASSPHRASE=test

pulumi login gs://eventrun-state
python iac.py
