import yaml
import re
import pulumi
import os
import glob
import uuid
import datetime
import base64
import json
import google.auth

from pulumi import resource, Output
from pulumi.automation import errors
from pulumi.metadata import get_stack
from pulumi_gcp import storage, bigquery, serviceaccount, projects, organizations, cloudbuild
from pulumi import automation as auto


def service_account(team: str):
    sa = serviceaccount.Account(
        team + '-service-account',
        account_id=team + '-service-account',
        display_name=team + ' - service account')
    iam = projects.IAMBinding(
        team + '-bq-admin-iam',
        members=[sa.email.apply(lambda email: f"serviceAccount:{email}")],
        role='roles/bigquery.admin')
    iam = projects.IAMBinding(
        team + '-cb-build-iam',
        members=[sa.email.apply(lambda email: f"serviceAccount:{email}")],
        role='roles/cloudbuild.builds.builder')
    return sa


def read_diff(path: str = '/workspace/DIFF_TEAM.txt'):
    with open(path, 'r') as file:
        return [item.strip() for item in file.readlines()]


def create_team_key(team: str, path: str = 'team_auth'):
    sa = service_account(team)
    key = serviceaccount.Key(
        team + '_key',
        service_account_id=sa.name,
        public_key_type="TYPE_X509_PEM_FILE")
    storage.BucketObject(
        team + '_key',
        name=team + '/' + team + '.json',
        bucket=path,
        content=key.private_key.apply(lambda x: base64.b64decode(x).decode('utf-8')))
    return key


def pulumi_program():
    print('hello')
    #key = create_team_key(team)


teams_diff = read_diff()
for team in teams_diff:
    stack = auto.create_or_select_stack(
        stack_name='init',
        project_name='eventrun',
        program=pulumi_program,
        work_dir='/workspace')
    stack.set_config("gpc:region", auto.ConfigValue("northamerica-northeast1"))
    stack.set_config("gcp:project", auto.ConfigValue("eventrun"))
    stack.refresh()
    print('##################### Preview Changes for Team: ' + team + ' #####################')
    stack.preview(on_output=print)
    print('##################### Upsert Changes for Team: ' + team + ' #####################')
    stack.up(on_output=print)
