# Default modules
import yaml
import re
import time
import base64

# Kubernetes library
from kubernetes import client, config, utils
from kubernetes.client import exceptions as k_exceptions

# RobotFramework Library
from robot.libraries.BuiltIn import BuiltIn


__author__ = "Rick Venema"
__version__ = 1.0


# Get testnamespace from robot config file
TESTNAMESPACE = BuiltIn().get_variable_value("${TEST_NAMESPACE}")


class CustomLibrary:
    ROBOT_LIBRARY_VERSION = __version__
    ROBOT_LIBRARY_SCOPE = 'GLOBAL'

    # PG Version Utils
    def version_into_yaml(self, yaml_location:str, yaml_output:str, version:str) -> None:
        """
        Function to put version into yaml file replacing the PG_VERSION tag with the given version.

        :param yaml_location: Location of the yaml file with PG_VERSION
        :param yaml_output: Location to write changed yaml file
        :param version: Version of postgresql
        :return: None
        """

        with open(yaml_location, "r") as f:
            data = f.read()

        data = re.sub(r'PG_VERSION', version, data)

        with open(yaml_output, "w") as f:
            f.write(data)
        # todo: check if file exists

    def process_pg_version(self, pg_version: str) -> str | None:
        """
        Function to return the previous minor version of the given version

        :param pg_version: version to process
        :return: None if minor is 0 and string with major.minor if minor >=1
        """
        minor = pg_version.split(".")[1]
        major = pg_version.split(".")[0]
        if minor == '0':
            return None
        else:
            return f"{major}.{int(minor)-1}"

    def get_version_postgres_select(self, raw) -> str:
        """
        Convert select version() to postgresql version
        :param raw: Raw output of RBFW Query keyword with select version();
        :return: String representation of PG version
        """
        return raw[0][0].split(" ")[1]


    # Cluster Utils
    def deploy_yaml_postgres(self, yaml_location: str) -> None:
        """
        Function to deploy postgresql yaml

        :param yaml_location: location of the yaml file
        :return: None
        """
        # Connect to openshift Api
        config.load_incluster_config()
        cnpg_group = 'postgresql.cnpg.io'
        cnpg_version = 'v1'
        cnpg_plural = 'clusters'
        COA_api = client.CustomObjectsApi()
        # Load yaml data
        with open(yaml_location, 'r') as f:
            data_loaded = yaml.safe_load(f)
        try:
            # todo: opvangen status deploy bucket
            COA_api.create_namespaced_custom_object(cnpg_group, cnpg_version, TESTNAMESPACE, cnpg_plural, data_loaded)
        except k_exceptions.ApiException as e:
            raise Exception("ApiException")

    def apply_yaml_postgres(self, clustername: str, yaml_location: str) -> None:
        """
        Function to configure running PG cluster (oc apply -f)

        :param clustername: Name of the running PG cluster
        :param yaml_location: Location of the new yaml to apply
        :return: None
        """
        config.load_incluster_config()
        cnpg_group = 'postgresql.cnpg.io'
        cnpg_version = 'v1'
        cnpg_plural = 'clusters'
        COA_api = client.CustomObjectsApi()
        with open(yaml_location, 'r') as f:
            data_loaded = yaml.safe_load(f)
        try:
            # todo: opvangen status deploy bucket
            # oc apply of new yaml data
            COA_api.patch_namespaced_custom_object(cnpg_group, cnpg_version, TESTNAMESPACE, cnpg_plural, clustername, data_loaded)
        except k_exceptions.ApiException as e:
            raise Exception("ApiException")

    def delete_postgres_yaml(self, cluster_name: str) -> str:
        """
        Function to delete cluster with name
        :param cluster_name: Name of the PG cluster
        :return: Status if success or not
        """
        config.load_incluster_config()
        cnpg_group = 'postgresql.cnpg.io'
        cnpg_version = 'v1'
        cnpg_plural = 'clusters'
        COA_api = client.CustomObjectsApi()
        out = COA_api.delete_namespaced_custom_object(cnpg_group, cnpg_version, TESTNAMESPACE, cnpg_plural,
                                                      cluster_name)
        return out['status']

    def check_until_postgres_cluster_deployed(self, clustername:str="cluster-example", threshold=5000) -> None:
        """
        Function to wait and check until a postgresql cluster is in "cluster in healthy state" phase
        :param clustername: Name of the cluster to monitor
        :param threshold:  amount of times the cluster is checked, if exceeded TimeoutError will be raised
        :return: TODO uitzoeken return wachten op pg cluster
        """
        config.load_incluster_config()
        api_inst = client.CustomObjectsApi()
        cnpg_group = 'postgresql.cnpg.io'
        cnpg_version = 'v1'
        cnpg_plural = 'clusters'
        api_response = api_inst.get_namespaced_custom_object(cnpg_group, cnpg_version, TESTNAMESPACE, cnpg_plural,
                                                             clustername)
        count: int = 0
        while api_response['status']['phase'] != "Cluster in healthy state" or count > threshold:
            api_response = api_inst.get_namespaced_custom_object(cnpg_group, cnpg_version, TESTNAMESPACE, cnpg_plural,
                                                                 clustername)
            time.sleep(5)
            count += 1
        if count > threshold:
            raise TimeoutError(f"Took more than {threshold} operations to check if cluster is healthy")


    # Bucket Utils
    def deploy_bucket_yaml(self, yaml_location:str) -> None:
        """
        Function to create S3 bucket

        # Todo: Omzetten naar cohesity (wanneer prod ready)
        :param yaml_location: Location of S3 yaml file
        :return: None
        """
        # Connect to object bucket api for openshift
        config.load_incluster_config()
        bucket_group = 'objectbucket.io'
        bucket_version = 'v1alpha1'
        bucket_plural = 'objectbucketclaims'
        COA_api = client.CustomObjectsApi()
        with open(yaml_location, 'r') as f:
            data_loaded = yaml.safe_load(f)
        try:
            # todo: opvangen status deploy bucket
            COA_api.create_namespaced_custom_object(bucket_group, bucket_version, TESTNAMESPACE, bucket_plural,
                                                    data_loaded)
        except k_exceptions.ApiException as e:
            raise Exception("ApiException")

    def delete_bucket_yaml(self, bucket_name: str) -> str:
        """
        Function to delete bucket based on name
        :param bucket_name: Name of the bucket
        :return: status of API call
        """
        config.load_incluster_config()
        bucket_group = 'objectbucket.io'
        bucket_version = 'v1alpha1'
        bucket_plural = 'objectbucketclaims'
        COA_api = client.CustomObjectsApi()
        # todo: better error handling for delete bucket
        out = COA_api.delete_namespaced_custom_object(bucket_group, bucket_version, TESTNAMESPACE, bucket_plural,
                                                      bucket_name)
        return out['status']


    # Job Utils
    def apply_job_yaml(self, yaml_name: str) -> None:
        """
        Function to apply job yaml with name of yaml file

        Default folder is workspace/source/deployment, this is where the deployment folder is stored during tests.

        :param yaml_name: Name of yaml file without path
        :return: None
        """
        config.load_incluster_config()
        k8s_client = client.ApiClient()
        yaml_file = f'/workspace/source/deployment/{yaml_name}'
        # todo: Uitzoeken of utils iets returned en betere fout afhandeling
        utils.create_from_yaml(k8s_client, yaml_file)

    def delete_job_yaml(self, job_name: str) -> str:
        """
        Function to delete job specified
        :param job_name: Name of the job to delete
        :return: Status of the deletion
        """
        config.load_incluster_config()
        api = client.BatchV1Api()
        out = api.delete_namespaced_job(job_name, TESTNAMESPACE, propagation_policy='Foreground')
        return out.status

    def wait_until_job_finished(self, job_name: str) -> None:
        """
        Function to wait until the specified job is completed

        TODO: implement timeout

        :param job_name: Name of the job to wait on
        :return: None
        """
        config.load_incluster_config()
        api_instance = client.BatchV1Api()
        api_response = api_instance.read_namespaced_job_status(job_name, TESTNAMESPACE)
        while not api_response.status.succeeded:
            api_response = api_instance.read_namespaced_job_status(job_name, TESTNAMESPACE)
            time.sleep(5)


    # Backup Utils
    def run_backup_yaml(self, yaml_location:str) -> None:
        """
        Function to apply backup yaml

        TODO: Uitzoeken run backup yaml met return statement
        :param yaml_location: Location of the yaml file with backup
        :return: None
        """
        config.load_incluster_config()
        cnpg_group = 'postgresql.cnpg.io'
        cnpg_version = 'v1'
        cnpg_plural = 'backups'
        COA_api = client.CustomObjectsApi()
        with open(yaml_location, 'r') as f:
            data_loaded = yaml.safe_load(f)
        try:
            COA_api.create_namespaced_custom_object(cnpg_group, cnpg_version, TESTNAMESPACE, cnpg_plural, data_loaded)
        except k_exceptions.ApiException as e:
            raise Exception("ApiException")

    def wait_until_backup_completed(self) -> None:
        """
        Function to wait until backup not running anymore

        It does not matter if the backup fails or not
        TODO: uitzoeken return wubc
        :return: None
        """
        config.load_incluster_config()
        cnpg_group = 'postgresql.cnpg.io'
        cnpg_version = 'v1'
        cnpg_plural = 'backups'
        COA_api = client.CustomObjectsApi()
        api_resp = COA_api.get_namespaced_custom_object(cnpg_group, cnpg_version, TESTNAMESPACE, cnpg_plural,
                                                        "automatic-test-backup")
        while api_resp['status']['phase'] == "running":
            time.sleep(5)
            api_resp = COA_api.get_namespaced_custom_object(cnpg_group, cnpg_version, TESTNAMESPACE, cnpg_plural,
                                                            "automatic-test-backup")

    def delete_backup(self, backupname: str) -> None:
        """
        Function to delete backup

        :param backupname: Name of the backup to delete
        :return: None
        """
        config.load_incluster_config()
        cnpg_group = 'postgresql.cnpg.io'
        cnpg_version = 'v1'
        cnpg_plural = 'backups'
        COA_api = client.CustomObjectsApi()
        out = COA_api.delete_namespaced_custom_object(cnpg_group, cnpg_version, TESTNAMESPACE, cnpg_plural, backupname)
        # todo: uitzoeken wat uit out precies teruggegeven moet worden

    def backup_completed(self) -> bool:
        """
        Function to check if backup completed or failed

        :return: Bool if backup completed
        """
        config.load_incluster_config()
        cnpg_group = 'postgresql.cnpg.io'
        cnpg_version = 'v1'
        cnpg_plural = 'backups'
        COA_api = client.CustomObjectsApi()
        api_resp = COA_api.get_namespaced_custom_object(cnpg_group, cnpg_version, TESTNAMESPACE, cnpg_plural,
                                                        "automatic-test-backup")
        if api_resp['status']['phase'] == "completed":
            return True
        elif api_resp['status']['phase'] == "failed":
            return False


    # ConfigMap Utils
    # todo: apply config map
    # todo: delete config map


    # HammerDB Utils
    def check_until_hammerdb_run_finished(self) -> None:
        """
        Function to wait until hammerdb performance test is completed

        TODO: uitzoeken of dit iets van een return nodig heeft
        :return: None
        """
        config.load_incluster_config()
        api_instance = client.BatchV1Api()
        name = 'j-runtprocc'
        api_response = api_instance.read_namespaced_job_status(name, TESTNAMESPACE)
        while not api_response.status.succeeded:
            api_response = api_instance.read_namespaced_job_status(name, TESTNAMESPACE)
            time.sleep(5)

    def get_perf_results_hammerdb(self) -> str:
        """
        Function to get the performance results of the hammerdb load job
        :return: performance result
        """
        config.load_incluster_config()
        api_v1_inst = client.CoreV1Api()
        lbl = 'job-name=j-runtprocc'
        pod_resp = api_v1_inst.list_namespaced_pod(TESTNAMESPACE, label_selector=lbl)
        run_name = pod_resp.items[0].metadata.name

        pod_logs = api_v1_inst.read_namespaced_pod_log(run_name, TESTNAMESPACE, container='run-tprocc')
        perf_res = re.search(r'TEST\sRESULT.+\n', pod_logs)[0]
        return perf_res


    # Pod Utils
    def get_instance_pods(self, clustername: str) -> list:
        """
        Function to get the pod names of a cluster

        :param clustername: Name of the cluster
        :return: List with instance names
        """
        config.load_incluster_config()
        cnpg_group = 'postgresql.cnpg.io'
        cnpg_version = 'v1'
        cnpg_plural = 'clusters'
        COA_api = client.CustomObjectsApi()

        resp = COA_api.get_namespaced_custom_object(cnpg_group, cnpg_version, TESTNAMESPACE, cnpg_plural, clustername)
        return resp['status']['instanceNames']

    def check_node_location_instances(self, instances: list) -> bool:
        """
        Function to check the node location of the pods given with the instances list
        :param instances: List with pod names
        :return: Boolean if all pods are on different nodes
        """
        config.load_incluster_config()
        v1_api = client.CoreV1Api()
        nodes = []
        for instance in instances:
            pod_info = v1_api.read_namespaced_pod(instance, TESTNAMESPACE)
            nodes.append(pod_info.spec.node_name[1])
        if len(set(nodes)) == 1:
            return False
        else:
            return True

    def get_primary_pod(self) -> str:
        """
        Function for getting the primary pod name

        :return: Name of primary pod
        """
        config.load_incluster_config()
        k8s_client = client.CoreV1Api()
        primary_pod = k8s_client.list_namespaced_pod(namespace=TESTNAMESPACE,
                                                     label_selector='cnpg.io/instanceRole=primary')
        return primary_pod.items[0].metadata.name

    def delete_pg_pod(self, pod_name: str):
        """
        Function for removing pg pod with the given name

        TODO: Uitzoeken of delete_NS_pod iets teruggeeft
        :param pod_name: name of the pod to remove
        :return: None
        """
        config.load_incluster_config()
        k8s_client = client.CoreV1Api()
        k8s_client.delete_namespaced_pod(name=pod_name, namespace=TESTNAMESPACE)


    # Extra utils
    def get_superuser_keys(self, clustername:str="cluster-example") -> tuple[str, str, str, str]:
        """
        Function to get superuser keys of the postgresql cluster

        # todo: opzoeken gebruik en clustername als param meegeven
        :return: Database connection parameters
        """
        config.load_incluster_config()
        api_instance = client.CoreV1Api()
        name = f'{clustername}-superuser'
        out = api_instance.read_namespaced_secret(name, TESTNAMESPACE)
        dbname = 'postgres'
        user = base64.b64decode(out.data['user'])
        password = base64.b64decode(out.data['password'])
        host = base64.b64decode(out.data['host'])
        return dbname, user.decode('utf-8'), password.decode('utf-8'), host.decode('utf-8')

    def check_if_wal_archived(self) -> bool:
        """
        Function to check if WAL archiving is functional

        TODO: zorgen dat primary pod gebruikt wordt
        :return: Boolean if WAL archiving is functional
        """
        config.load_incluster_config()
        v1_api = client.CoreV1Api()
        pod_logs_primary = v1_api.read_namespaced_pod_log('cluster-example-1', TESTNAMESPACE, container='postgres')
        if "Archived WAL file" in pod_logs_primary:
            return True
        else:
            return False

    def check_if_postgres_in_repl(self, repl: list) -> bool:
        """
        Function to check if the postgresql cluster is in sync replication
        :param repl: the output of the `SELECT sync_state FROM pg_stat_replication;` query
        :return: Boolean with in sync repl
        """
        # repl = [('quorum',), ('async',)]
        if ('quorum',) in repl:
            return True
        else:
            return False


