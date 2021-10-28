import re
import sys
import pulumi
import os
import base64
import google.auth

from google.cloud import storage as gcs
from pulumi.automation import errors
from pulumi_gcp import storage, serviceaccount, projects, cloudbuild
from pulumi import automation as auto


def service_account(team: str, postfix='-service-account'):
    sa = serviceaccount.Account(
        team + postfix,
        account_id=team + postfix,
        display_name=team + ' - service account')
    token = serviceaccount.get_account_access_token(
        target_service_account='tsbt-service-account@eventrun.iam.gserviceaccount.com',
        scopes=['cloud-platform']
    )
    projects.IAMMember(
        team + '-storage-admin-iam',
        member=sa.email.apply(lambda e: f"serviceAccount:{e}"),
        role='roles/storage.admin')
    projects.IAMMember(
        team + '-bq-admin-iam',
        member=sa.email.apply(lambda e: f"serviceAccount:{e}"),
        role='roles/bigquery.admin')
    projects.IAMMember(
        team + '-cloudbuild-editor-iam',
        member=sa.email.apply(lambda e: f"serviceAccount:{e}"),
        role='roles/cloudbuild.builds.editor'
    )
    return sa


def list_manifests(root: str):
    yml_list = []
    for path, subdirs, files in os.walk(root):
        for name in files:
            if name.endswith('.yaml') or name.endswith('.yml'):
                yml_list.append(path + '/' + name)
    return yml_list


def create_team_key(team: str, path: str = 'team_auth'):
    sa = service_account(team)
    pulumi.export(team + '_sa', sa.name)
    key = serviceaccount.Key(
        team + '_key',
        service_account_id=sa.name,
        private_key_type='TYPE_GOOGLE_CREDENTIALS_FILE',
        public_key_type="TYPE_X509_PEM_FILE")
    storage.BucketObject(
        team + '_key',
        name=team + '/' + team + '.json',
        bucket=path,
        content=key.private_key.apply(lambda x: base64.b64decode(x).decode('utf-8')))
    return key


def get_teams(root: str = '/workspace/teams/'):
    manifests_set = list_manifests(root)
    teams_set = set([
        re.search('teams/(.+?)/+', team).group(1)
        for team in manifests_set
        if re.search('teams/(.+?)/+', team)
    ])
    return teams_set


def pulumi_program():
    key = create_team_key(team)
    pulumi.export(team + '_key', key.private_key.apply(lambda x: base64.b64decode(x).decode('utf-8')))



if __name__ == "__main__":
    team = sys.argv[1]
    stack = auto.create_or_select_stack(
            stack_name=team + '_sa',
            project_name='eventrun',
            program=pulumi_program,
            work_dir='/workspace')
    stack.set_config("gpc:region", auto.ConfigValue("northamerica-northeast1"))
    stack.set_config("gcp:project", auto.ConfigValue("eventrun"))
    stack.refresh()
    preview = stack.preview()
    up = stack.up(on_output=print)
    credentials, project_id = google.auth.default()
    bq_client = gcs.Client()
    with open(team + '.json', 'wb') as file_obj:
        bq_client.download_blob_to_file('gs://team_auth/' + team + '/' + team + '.json', file_obj)

