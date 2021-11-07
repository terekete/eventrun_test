import re
import sys
import pulumi
import pulumi_gcp as gcp
import os
import base64
import google.auth
from pulumi_gcp.compute import network

from google.cloud import storage as gcs
from pulumi.automation import errors
from pulumi_gcp import storage, serviceaccount, projects, bigquery, dataproc, compute
from pulumi import automation as auto


def service_account(team: str, postfix='-service-account'):
    sa = serviceaccount.Account(
        team + postfix,
        account_id=team + postfix,
        display_name=team + ' - service account')
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
        role='roles/cloudbuild.builds.editor')
    projects.IAMMember(
        team + '-custom-view-access-iam',
        member=sa.email.apply(lambda e: f"serviceAccount:{e}"),
        role='projects/eventrun/roles/CustomViewsAccessor'
    )
    projects.IAMMember(
        team + '-custom-compute-network',
        member=sa.email.apply(lambda e: f"serviceAccount:{e}"),
        role='projects/eventrun/roles/CustomComputeAccessor'
    )
    return sa


def list_manifests(root: str):
    yml_list = []
    for path, subdirs, files in os.walk(root):
        for name in files:
            if name.endswith('.yaml') or name.endswith('.yml'):
                yml_list.append(path + '/' + name)
    return yml_list


def create_team_key(sa, team: str, path: str = 'team_auth'):
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


def create_team_token(sa, team: str, path: str = 'team_auth'):
    token = serviceaccount.get_account_access_token(
            target_service_account=sa.email,
            scopes=['cloud-platform'])
    # storage.BucketObject(
    #     team + '_token',
    #     name=team + '/' + team + '.token',
    #     bucket=path,
    #     content=token.access_token)
    return token
    

def get_teams(root: str = '/workspace/teams/'):
    manifests_set = list_manifests(root)
    teams_set = set([
        re.search('teams/(.+?)/+', team).group(1)
        for team in manifests_set
        if re.search('teams/(.+?)/+', team)
    ])
    return teams_set



def pulumi_program():
    sa = service_account(team)
    # key = create_team_key(sa, team)
    #token = create_team_token(sa, team)
    pr = gcp.Provider(
        team + '-provider',
        impersonate_service_account=sa.email,
        region='northamerica-northeast1',
        project='eventrun')
    bucket = storage.Bucket(
        team + '_test_bucket',
        name=team + '_test_bucket_eventrun',
        force_destroy=True,
        storage_class='STANDARD',
        location="northamerica-northeast1",
        opts=pulumi.ResourceOptions(provider=pr)
    )
    net = compute.Network(team + '-network',
        auto_create_subnetworks=False,
        routing_mode='REGIONAL',
        opts=pulumi.ResourceOptions(provider=pr, parent=sa)
    )
    subnet = compute.Subnetwork(
        team + '-subnetwork',
        ip_cidr_range=('10.2.0.0/16' if team == 'ea' else '10.1.0.0/16'),
        region='northamerica-northeast1',
        network=net.id,
        private_ip_google_access=True,
        opts=pulumi.ResourceOptions(provider=pr, parent=net)
    )
    dts = bigquery.Dataset(
        resource_name=team + '_new_dataset',
        dataset_id=team + '_new_dataset',
        description=team + '_new_dataset',
        delete_contents_on_destroy=False,
        location='northamerica-northeast1',
        opts=pulumi.ResourceOptions(provider=pr)
    )
    vw = bigquery.Table(
        resource_name=team + '_test_run_ext',
        table_id=team +'_test_run_ext',
        dataset_id=dts.dataset_id,
        deletion_protection=False,
        view=bigquery.TableViewArgs(
            query='select * from `intrepid-memory-321513.test_dataset.test_table_ext` limit 10',
            use_legacy_sql=False
        ),
        opts=pulumi.ResourceOptions(provider=pr)
    )
    vw2 = bigquery.Table(
        resource_name=team + '_test_run_ext2',
        table_id=team +'_test_run_ext2',
        dataset_id=dts.dataset_id,
        deletion_protection=False,
        view=bigquery.TableViewArgs(
            query='select * from `intrepid-memory-321513.test_dataset.test_table_ext` limit 10',
            use_legacy_sql=False
        ),
        opts=pulumi.ResourceOptions(provider=pr)
    )
    projects.IAMMember(
        team + '-bq-data-viewer-iam',
        member='user:gates.mark@gmail.com',
        role='roles/bigquery.dataViewer')
    projects.IAMMember(
        team + '-project-viewer-iam',
        member='user:gates.mark@gmail.com',
        role='roles/viewer')
    read_vw = bigquery.IamBinding(
            resource_name=team + '_vw_read_iam',
            dataset_id=vw2.dataset_id,
            table_id=vw2.table_id,
            role='roles/bigquery.dataViewer',
            members=['user:gates.mark@gmail.com'],
            opts=pulumi.ResourceOptions(provider=pr, parent=vw2))
    writer_vw = bigquery.IamBinding(
            resource_name=team + '_vw_writer_iam',
            dataset_id=vw2.dataset_id,
            table_id=vw2.table_id,
            role='roles/bigquery.dataEditor',
            members=['user:gates.mark@gmail.com'],
            opts=pulumi.ResourceOptions(provider=pr, parent=vw2))
    scheduled = bigquery.DataTransferConfig(
        resource_name=team + '_test_scheduled',
        display_name=team + '_test_scheduled',
        data_source_id='scheduled_query',
        schedule='every 25 minutes',
        destination_dataset_id='tsbt_new_dataset',
        location='northamerica-northeast1',
        params={
            'destination_table_name_template': team + '_test_scheduled',
            'write_disposition': 'WRITE_TRUNCATE',
            'query': 'select * from `intrepid-memory-321513.test_dataset.test_table_ext` limit 10'
        },
        opts=pulumi.ResourceOptions(provider=pr)
    )
    proc = dataproc.Cluster(team + '-dataproc-cluster',
        region='northamerica-northeast1',
        graceful_decommission_timeout='120s',
        labels ={
            'foo': 'bar'
        },
        cluster_config=dataproc.ClusterClusterConfigArgs(
            staging_bucket=bucket.id,
            master_config=dataproc.ClusterClusterConfigMasterConfigArgs(
                num_instances=1,
                machine_type='e2-standard-4',
                disk_config=dataproc.ClusterClusterConfigMasterConfigDiskConfigArgs(
                    boot_disk_type='pd-ssd',
                    boot_disk_size_gb=10,
                ),
            ),
            worker_config=dataproc.ClusterClusterConfigWorkerConfigArgs(
                num_instances=2,
                machine_type='e2-standard-2',
                disk_config=dataproc.ClusterClusterConfigWorkerConfigDiskConfigArgs(
                    boot_disk_size_gb=10,
                    num_local_ssds=1,
                ),
            ),
            preemptible_worker_config=dataproc.ClusterClusterConfigPreemptibleWorkerConfigArgs(
                num_instances=0,
            ),
            software_config=dataproc.ClusterClusterConfigSoftwareConfigArgs(
                image_version='1.3.7-deb9',
                override_properties={
                    'dataproc:dataproc.allow.zero.workers': 'true',
                },
            ),
            gce_cluster_config=dataproc.ClusterClusterConfigGceClusterConfigArgs(
                service_account_scopes=['cloud-platform'],
                internal_ip_only=True,
                # network=net.id,
                subnetwork=subnet.id
            ),
        ),
        opts=pulumi.ResourceOptions(provider=pr)
    )
    # pulumi.export(team + '_key', key.private_key.apply(lambda x: base64.b64decode(x).decode('utf-8')))
    # pulumi.export(team + '_token', token.access_token)















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

    # credentials, project_id = google.auth.default()
    # bq_client = gcs.Client()
    # with open(team + '.json', 'wb') as file_obj:
    #     bq_client.download_blob_to_file('gs://team_auth/' + team + '/' + team + '.json', file_obj)
    # with open(team, 'wb') as file_obj:
    #     bq_client.download_blob_to_file('gs://team_auth/' + team + '/' + team + '.token', file_obj)



