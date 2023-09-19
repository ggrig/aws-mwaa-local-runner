# DAG exhibiting task flow paradigm in airflow 2.0
# https://airflow.apache.org/docs/apache-airflow/2.0.2/tutorial_taskflow_api.html
# Modified for our use case

import json
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

import logging

logger = logging.getLogger(__name__)

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'harut',
}
from airflow.providers.amazon.aws.operators.ecs import (
    EcsCreateClusterOperator,
    EcsDeleteClusterOperator,
    EcsDeregisterTaskDefinitionOperator,
    EcsRegisterTaskDefinitionOperator,
    EcsRunTaskOperator,
)
@dag(default_args=default_args, schedule_interval="@daily", start_date=days_ago(2), tags=['example'])
def ecs_deployment_test():

    @task()
    def test_context():
        logger.info(">> test_context")
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
