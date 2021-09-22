import yaml
import re
import pulumi
import os
import glob
import uuid
import datetime

from pulumi import resource
from pulumi.automation import errors
from pulumi.metadata import get_stack
from pulumi_gcp import storage, bigquery, serviceaccount, projects, organizations
from pulumi import automation as auto
from cerberus import Validator


def dataset(manifest: str):
    dts = bigquery.Dataset(
        resource_name=manifest['resource_name'],
        dataset_id=manifest['dataset_id'],
        description=manifest['description'],
        labels={
            'cost_center': manifest['metadata']['cost_center'],
            'dep': manifest['metadata']['dep'],
            'bds': manifest['metadata']['bds'],
        },
        default_table_expiration_ms=manifest['table_expiration_ms'],
        location='northamerica-northeast1'
    )



def dataset_user_access(
    manifest: str,
    role: str):

    # readers = ["users:" + reader for reader in manifest['users']['readers']]
    # writers = ["users:" + writer for writer in manifest['users']['writers']]
    bigquery.DatasetAccess(
        resource_name=manifest['resource_name'],
        dataset_id=manifest['dataset_id'],
        user_by_email=user,
        role=role
    )


def validate_dataset_manifest(manifest: str):
    schema = eval(open('./schemas/dataset.py', 'r').read())
    validator = Validator(schema)
    try:
        if validator.validate(manifest, schema):
            return
    except:
        print("##### Dataset Exception - " + manifest['dataset_id'])
        raise auto.InlineSourceRuntimeError(validator.errors)


def table(manifest: str):
    tbl = bigquery.Table(
        resource_name=manifest['resource_name'],
        dataset_id=manifest['dataset_id'],
        table_id=manifest['table_id'],
        deletion_protection=False,
        expiration_time=manifest['expiration_ms'],
        friendly_name=manifest['friendly_name'],
        labels={
            'cost_center': manifest['metadata']['cost_center'],
            'dep': manifest['metadata']['dep'],
            'bds': manifest['metadata']['bds'],
        },
        schema=manifest['schema']
    )


def table_user_access(
    manifest: str,
    role: str = 'roles/bigquery.dataViewer'):

    readers = ["users:" + reader for reader in manifest['users']['readers']]
    writers = ["users:" + writer for writer in manifest['users']['writers']]
    bigquery.IamBinding(
        resource_name=manifest['resource_name'],
        dataset_id=manifest['dataset_id'],
        table_id=manifest['table_id'],
        role=role,
        members=readers
    )
    bigquery.IamBinding(
        resource_name=manifest['resource_name'],
        dataset_id=manifest['dataset_id'],
        table_id=manifest['table_id'],
        role=role,
        members=writers
    )


def validate_table_manifest(manifest: str):
    schema = eval(open('./schemas/table.py', 'r').read())
    validator = Validator(schema)
    try:
        if validator.validate(manifest, schema):
            return
    except:
        print("##### Table Exception - " + manifest['table_id'])
        raise auto.InlineSourceRuntimeError(validator.errors)


def scheduled(manifest: str, sa=None):
    # hex = uuid.uuid4().hex
    # dt = datetime.datetime.today()
    # sql = f'select {hex}, {dt}, '
    bigquery.DataTransferConfig(
        resource_name=manifest['resource_name'],
        display_name=manifest['display_name'],
        data_source_id=manifest['data_source_id'],
        schedule=manifest['schedule'],
        destination_dataset_id=manifest['destination_dataset_id'],
        location='northamerica-northeast1',
        params={
            'destination_table_name_template': manifest['params']['destination_table_name'],
            'write_disposition': manifest['params']['write_disposition'],
            'query': manifest['params']['query']
        },
        service_account_name=sa.email)


def validate_scheduled_manifest(manifest: str):
    schema = eval(open('./schemas/scheduled.py').read())
    validator = Validator(schema)
    try:
        if validator.validate(manifest, schema):
            return
    except:
        print("##### Scheduled Exception - " + manifest['display_name'])
        raise auto.InlineSourceRuntimeError(validator.errors)


def create_sa(team: str):
    return serviceaccount.Account(
        team + '-sa',
        account_id=team + '-sa',
        display_name=team + '-sa - service account')


def set_iam_sa(sa):
    iam = projects.IAMBinding(
        team + '-bq-admin-iam',
        condition=projects.IAMBindingConditionArgs(
            description=team + '-bq-admin-iam',
            expression='request.time < timestamp(\"2021-01-01T00:00:00Z\")',
            title='bq-admin-iam-expiration'),
        members=[sa.email.apply(lambda email: f"serviceAccount:{email}")],
        role='roles/bigquery.admin')
    iam = projects.IAMBinding(
        team + '-project-admin-iam',
        condition=projects.IAMBindingConditionArgs(
            description=team + '-project-admin-iam',
            expression='request.time < timestamp(\"2021-01-01T00:00:00Z\")',
            title='project-admin-iam-expiration'),
        members=[sa.email.apply(lambda email: f"serviceAccount:{email}")],
        role='roles/resourcemanager.projectIamAdmin')
    return sa


def get_sa(team):
    return set_iam_sa(create_sa(team))


def read_yml(path: str):
    file = open(path, 'r')
    try:
        return yaml.safe_load(file)
    except yaml.YAMLError as e:
        raise e


def read_diff(path: str = '/workspace/DIFF_TEAM.txt'):
    with open(path, 'r') as file:
        return [item.strip() for item in file.readlines()]


def list_manifest(root: str):
    yml_list = []
    for path, subdirs, files in os.walk(root):
        for name in files:
            if name.endswith('.yaml'):
                yml_list.append(path, '/' + name)
    return yml_list


def read_yml(path: str):
    file = open(path, 'r')
    try:
        return yaml.safe_load(file)
    except yaml.YAMLError as e:
        raise e


def read_diff(path: str = '/workspace/DIFF_TEAM.txt'):
    with open(path, 'r') as file:
        return [item.strip() for item in file.readlines()]


def list_manifests(root: str):
    yml_list = []
    for path, subdirs, files in os.walk(root):
        for name in files:
            if name.endswith('.yaml') or name.endswith('.yml'):
                yml_list.append(path + '/' + name)
    return yml_list


def pulumi_program():
    context = {
        'team_stack': pulumi.get_stack(),
        'sa': get_sa(pulumi.get_stack()),
        'project': pulumi.get_project()
    }

    for dataset_path in datasets_list:
        if re.search('/workspace/teams/(.+?)/+', dataset_path).group(1) == context['team_stack']:
            update(dataset_path, context)
    for table_path in tables_list:
        if re.search('/workspace/teams/(.+?)/+', table_path).group(1) == context['team_stack']:
            update(table_path, context)
    for query_path in scheduled_list:
        if re.search('/workspace/teams/(.+?)/+', query_path).group(1) == context['team_stack']:
            update(query_path, context)


def update(path:str, context=None):
    yml = read_yml(path)

    try:
        if yml and yml['kind'] == 'dataset':
            validate_dataset_manifest(yml)
            dataset(yml)
        if yml and yml['kind'] == 'table':
            validate_table_manifest(yml)
            table(yml)
        if yml and yml['kind'] == 'scheduled':
            validate_scheduled_manifest(yml)
            scheduled(yml)
    except auto.errors.CommandError as e:
        raise e


def get_kind(
    manifest: str,
    kind: str):

    with open(manifest) as file:
        yml = yaml.safe_load(file)
        if yml and yml['kind'] == kind:
            return manifest




teams_root = '/workspace/teams/'
manifests_set = list_manifests(teams_root)
datasets_list = []
tables_list = []
scheduled_list = []


for manifest in manifests_set:
    if get_kind(manifest, 'dataset'):
        datasets_list.append(manifest)
    elif get_kind(manifest, 'table'):
        tables_list.append(manifest)
    elif get_kind(manifest, 'scheduled'):
        scheduled_list.append(manifest)


teams_set = set([
    re.search('teams/(.+?)/+', team).group(1)
    for team in manifests_set
    if re.search('teams/(.+?)/+', team)
])


teams_diff = read_diff()
for team in teams_diff:
    print('#####################')
    stack = auto.create_or_select_stack(
        stack_name=team,
        project_name='eventrun',
        program=pulumi_program,
        work_dir='/workspace')
    stack.set_config("gpc:region", auto.ConfigValue("northamerica-northeast1"))
    stack.set_config("gcp:project", auto.ConfigValue("eventrun"))
    stack.refresh(on_output=print)
    stack.preview(on_output=print)
    stack.up(on_output=print)
