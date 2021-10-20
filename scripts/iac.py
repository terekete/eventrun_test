import yaml
import re
import pulumi
import sys
import os
import base64
import time
import re

from collections import defaultdict, namedtuple
from pulumi import Output
from pulumi_gcp import storage, bigquery
from pulumi import automation as auto
from cerberus import Validator


def validate_resource_name(
    resource_name,
    team: str,
    prefix: str = None):

    if prefix:
        return (
            resource_name
            if re.search('(' + prefix + ')_(' + team + ')_(.*)', resource_name)
            else prefix + '_' + team + '_' + resource_name
        )
    else:
        return (
            resource_name
            if re.search('(' + team + ')_(.*)', resource_name)
            else team + '_' + resource_name
        )


def validate_manifest(
    manifest: str,
    schema_path: str):

    schema = eval(open(schema_path).read())
    validator = Validator(schema)
    try:
        if validator.validate(manifest, schema):
            return
    except:
        print("##### Schema Exception - " + manifest['resource_name'].lower())
        raise auto.InlineSourceRuntimeError(validator.errors)


results = namedtuple('results', ['sorted', 'cyclic'])
def graph_sort(l: str):
    n_heads = defaultdict(int)
    tails = defaultdict(list)
    heads = []
    for h, t in l:
        n_heads[t]+=1
        if h in tails:
            tails[h].append(t)
        else:
            tails[h] = [t]
            heads.append(h)
    ordered = [h for h in heads if h not in n_heads]
    for h in ordered:
        for t in tails[h]:
            n_heads[t]-=1
            if not n_heads[t]:
                ordered.append(t)
    cyclic = [n for n, heads in n_heads.items() if heads]
    return results(ordered, cyclic)


def dataset(
    manifest: str,
    team: str,
    delay: int = 5):

    validate_manifest(manifest, './schemas/dataset.py')

    dts = bigquery.Dataset(
        resource_name=manifest['resource_name'].lower() + '_dataset',
        dataset_id=validate_resource_name(manifest['resource_name'].lower(), team),
        description=manifest['description'],
        delete_contents_on_destroy=False,
        labels={
            'cost_center': manifest['metadata']['cost_center'],
            'dep': manifest['metadata']['dep'],
            'bds_email': manifest['metadata']['bds_email'].lower(),
        },
        default_table_expiration_ms=manifest['table_expiration_ms'],
        location='northamerica-northeast1'
    )
    readers = [
        reader.replace('user:', '').replace('serviceAccount:', '') 
        for reader in manifest['users']['readers']
    ]
    writers = [
        writer.replace('user:', '').replace('serviceAccount:', '') 
        for writer in manifest['users']['writers']
    ]
    for reader in readers:
        bigquery.DatasetAccess(
            resource_name=manifest['resource_name'] + '_reader_iam',
            dataset_id=dts.dataset_id,
            user_by_email=reader,
            role='roles/bigquery.dataViewer'
        )
    for writer in writers:
        bigquery.DatasetAccess(
            resource_name=manifest['resource_name'] + '_writer_iam',
            dataset_id=dts.dataset_id,
            user_by_email=writer,
            role='roles/bigquery.dataEditor'
        )
    time.sleep(delay)


def table(
    manifest: str,
    team: str,
    delay: int = 1,
    prefix: str = 'bq'):

    validate_manifest(manifest, './schemas/table.py')

    readers = [reader for reader in manifest['users']['readers'] or []]
    writers = [writer for writer in manifest['users']['writers'] or []]

    tbl = bigquery.Table(
        resource_name=manifest['resource_name'].lower() + '_table',
        table_id=validate_resource_name(manifest['resource_name'].lower(), team, prefix),
        dataset_id=manifest['dataset_id'].lower(),
        deletion_protection=False,
        expiration_time=manifest['expiration_ms'],
        labels={
            'cost_center': manifest['metadata']['cost_center'],
            'dep': manifest['metadata']['dep'],
            'bds_email': manifest['metadata']['bds_email'].lower(),
        },
        schema=manifest['schema']
    )
    if readers:
        bigquery.IamBinding(
            resource_name=manifest['resource_name'] + '_read_iam',
            dataset_id=tbl.dataset_id,
            table_id=tbl.table_id,
            role='roles/bigquery.dataViewer',
            members=readers
        )
    if writers:
        bigquery.IamBinding(
            resource_name=manifest['resource_name'] + '_write_iam',
            dataset_id=tbl.dataset_id,
            table_id=tbl.table_id,
            role='roles/bigquery.dataEditor',
            members=writers
        )
        time.sleep(delay)


def materialized(
    manifest: str,
    team: str,
    delay: int = 1,
    prefix: str = 'bq'):

    validate_manifest(manifest, './schemas/materialized.py')

    readers = [reader for reader in manifest['users']['readers'] or []]
    writers = [writer for writer in manifest['users']['writers'] or []]

    mat = bigquery.TableMaterializedViewArgs(
        query=manifest['params']['query'],
        enable_refresh=manifest['params']['refresh'],
        refresh_interval_ms=manifest['params']['refresh_ms']
    )
    tbl = bigquery.Table(
        resource_name=manifest['resource_name'],
        dataset_id=manifest['dataset_id'],
        table_id=validate_resource_name(manifest['resource_name'].lower(), team, prefix),
        deletion_protection=False,
        expiration_time=manifest['expiration_ms'],
        labels={
            'cost_center': manifest['metadata']['cost_center'],
            'dep': manifest['metadata']['dep'],
            'bds_email': manifest['metadata']['bds_email'].lower(),
        },
        materialized_view=mat
    )
    if readers:
        bigquery.IamBinding(
            resource_name=manifest['resource_name'] + '_read_iam',
            dataset_id=tbl.dataset_id,
            table_id=tbl.table_id,
            role='roles/bigquery.dataViewer',
            members=readers
        )
    if writers:
        bigquery.IamBinding(
            resource_name=manifest['resource_name'] + '_write_iam',
            dataset_id=tbl.dataset_id,
            table_id=tbl.table_id,
            role='roles/bigquery.dataEditor',
            members=writers
        )
    time.sleep(delay)


def scheduled(
    manifest: str,
    team: str,
    delay: int = 1,
    prefix: str = 'bq'):

    validate_manifest(manifest, './schemas/scheduled.py')

    scheduled = bigquery.DataTransferConfig(
        resource_name=manifest['resource_name'].lower(),
        display_name=manifest['resource_name'].lower(),
        data_source_id='scheduled_query',
        schedule=manifest['schedule'],
        destination_dataset_id=manifest['destination_dataset_id'],
        location='northamerica-northeast1',
        params={
            'destination_table_name_template': validate_resource_name(manifest['destination_table_id'], team, prefix),
            'write_disposition': manifest['write_disposition'],
            'query': manifest['query']
        })
    time.sleep(delay)


def bucket(
    manifest: str,
    team: str,
    delay: int = 1,
    prefix: str = 'gcs'):

    validate_manifest(manifest, './schemas/bucket.py')

    readers = [reader for reader in manifest['users']['readers'] or []]
    writers = [writer for writer in manifest['users']['writers'] or []]
    

    bucket = storage.Bucket(
        manifest['resource_name'].lower() + '_bucket',
        name=validate_resource_name(manifest['resource_name'], team, prefix),
        force_destroy=True,
        storage_class='STANDARD' if not manifest['storage_class'] else manifest['storage_class'],
        lifecycle_rules=[storage.BucketLifecycleRuleArgs(
            action=storage.BucketLifecycleRuleActionArgs(
                type='Delete' if not manifest['lifecycle_type'] else manifest['lifecycle_type']
            ),
            condition=storage.BucketLifecycleRuleConditionArgs(
                age=90 if not manifest['lifecycle_age_days'] else manifest['lifecycle_age_days']
            ),
        )],
        location="northamerica-northeast1",
        labels={
            'cost_center': manifest['metadata']['cost_center'],
            'dep': manifest['metadata']['dep'],
            'bds_email': manifest['metadata']['bds_email'].lower(),
        }
    )
    if readers:
        storage.BucketIAMBinding(
            resource_name=manifest['resource_name'] + '_read_iam',
            bucket=bucket.id,
            role="roles/storage.objectViewer",
            members=readers)
    if writers:
        storage.BucketIAMBinding(
            resource_name=manifest['resource_name'] + '_write_iam',
            bucket=bucket.id,
            role="roles/storage.objectAdmin",
            members=writers)
    time.sleep(delay)


def read_yml(path: str):
    file = open(path, 'r')
    try:
        return yaml.safe_load(file)
    except yaml.YAMLError as e:
        raise e


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


def update(
    path: str,
    team: str):

    yml = read_yml(path)
    try:
        if yml and yml['kind'] == 'dataset':
            dataset(yml, team)
        if yml and yml['kind'] == 'table':
            table(yml, team)
        if yml and yml['kind'] == 'materialized':
            materialized(yml, team)
        if yml and yml['kind'] == 'scheduled':
            scheduled(yml, team)
        if yml and yml['kind'] == 'bucket':
            bucket(yml, team)
    except auto.errors.CommandError as e:
        raise e


def get_manifast_kind(
    manifest: str,
    kind: str):

    with open(manifest) as file:
        yml = yaml.safe_load(file)
        if yml and yml['kind'] == kind:
            return manifest


def get_value(
    manifest: str,
    key: str):

    with open(manifest) as file:
        yml = yaml.safe_load(file)
        if yml:
            return yml[key]


def render_user_data(key) -> Output:
    temp = base64.b64encode(key.encode("utf-8"))
    temp_str = str(temp, "utf-8")
    return temp_str


def pulumi_program():
    sorted_path = graph_sort(dependency_map).sorted
    sorted_path.extend(list(set(manifests_set) - set(graph_sort(dependency_map).sorted)))
    team = pulumi.get_stack()
    for path in sorted_path:
        if re.search('/workspace/teams/(.+?)/+', path).group(1) == team:
            update(path, team)


if __name__ == "__main__":
    teams_root = '/workspace/teams/'
    manifests_set = list_manifests(teams_root)
    dependency_map = list(set([
        (root_manifest, dep_manifest)
        for dep_manifest in manifests_set
        for root_manifest in manifests_set
        if get_value(dep_manifest, 'dependencies')
        and get_value(root_manifest, 'resource_name')
        and get_value(root_manifest, 'resource_name') in get_value(dep_manifest, 'dependencies')
        and root_manifest != dep_manifest
    ]))
    team = sys.argv[1]
    stack = auto.create_or_select_stack(
        stack_name=team,
        project_name='eventrun',
        program=pulumi_program,
        work_dir='/workspace')
    stack.set_config("gpc:region", auto.ConfigValue("northamerica-northeast1"))
    stack.set_config("gcp:project", auto.ConfigValue("eventrun"))
    print('##################### IAC Changes for Team: ' + team + ' #####################')
    stack.refresh()
    preview = stack.preview()
    up = stack.up(on_output=print)

