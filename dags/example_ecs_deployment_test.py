# DAG exhibiting task flow paradigm in airflow 2.0
# https://airflow.apache.org/docs/apache-airflow/2.0.2/tutorial_taskflow_api.html
# Modified for our use case

import json
from airflow import DAG
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.hooks.ecs import EcsClusterStates, EcsTaskStates
from airflow.providers.amazon.aws.operators.ecs import (
    EcsCreateClusterOperator,
    EcsDeleteClusterOperator,
    EcsDeregisterTaskDefinitionOperator,
    EcsRegisterTaskDefinitionOperator,
    EcsRunTaskOperator,
)
from airflow.providers.amazon.aws.sensors.ecs import (
    EcsClusterStateSensor,
    EcsTaskDefinitionStateSensor,
    EcsTaskStateSensor,
)
from airflow.utils.trigger_rule import TriggerRule
from tests_system_providers_amazon_aws_utils import ENV_ID_KEY, SystemTestContextBuilder


import logging

logger = logging.getLogger(__name__)

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'harut',
}

# Externally fetched variables:
EXISTING_CLUSTER_NAME_KEY = "CLUSTER_NAME"
EXISTING_CLUSTER_SUBNETS_KEY = "SUBNETS"

sys_test_context_task = (
    SystemTestContextBuilder()
    # NOTE:  Creating a functional ECS Cluster which uses EC2 requires manually creating
    # and configuring a number of resources such as autoscaling groups, networking
    # etc. which is out of scope for this demo and time-consuming for a system test
    # To simplify this demo and make it run in a reasonable length of time as a
    # system test, follow the steps below to create a new cluster on the AWS Console
    # which handles all asset creation and configuration using default values:
    # 1. https://us-east-1.console.aws.amazon.com/ecs/home?region=us-east-1#/clusters
    # 2. Select "EC2 Linux + Networking" and hit "Next"
    # 3. Name your cluster in the first field and click Create
    .add_variable(EXISTING_CLUSTER_NAME_KEY)
    .add_variable(EXISTING_CLUSTER_SUBNETS_KEY, split_string=True)
    .build()
)


@dag(default_args=default_args, schedule_interval="@daily", start_date=days_ago(2), tags=['example'])
def ecs_deployment_test():

    @task()
    def test_context():
        logger.info(">> test_context")
        sys_test_context_task()
        logger.info("<< test_context")        
    @task()
    def aws_region():
        logger.info(">> aws_region")
        logger.info("<< aws_region") 
    @task()
    def create_cluster():
        logger.info(">> create_cluster")
        logger.info("<< create_cluster")
    @task()
    def await_cluster():
        logger.info(">> await_cluster")
        logger.info("<< await_cluster") 
    @task()
    def register_task():
        logger.info(">> register_task")
        logger.info("<< register_task") 
    @task()
    def await_task_definition():
        logger.info(">> await_task_definition")
        logger.info("<< await_task_definition") 
    @task()
    def run_task():
        logger.info(">> run_task")
        logger.info("<< run_task") 
    @task()
    def await_task_finish():
        logger.info(">> await_task_finish")
        logger.info("<< await_task_finish") 
    @task()
    def deregister_task():
        logger.info(">> deregister_task")
        logger.info("<< deregister_task") 
    @task()
    def delete_cluster():
        logger.info(">> delete_cluster")
        logger.info("<< delete_cluster") 
    @task()
    def await_delete_cluster():
        logger.info(">> await_delete_cluster")
        logger.info("<< await_delete_cluster") 

    test_context()
    aws_region()
    create_cluster()
    await_cluster()
    register_task()
    await_task_definition()
    run_task()
    await_task_finish()
    deregister_task()
    delete_cluster()
    await_delete_cluster()

ecs_deployment_test = ecs_deployment_test()
