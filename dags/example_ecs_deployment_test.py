# DAG exhibiting task flow paradigm in airflow 2.0
# https://airflow.apache.org/docs/apache-airflow/2.0.2/tutorial_taskflow_api.html
# Modified for our use case

import json
import boto3
from time import sleep
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
from tests_system_providers_amazon_aws_utils import (
    ENV_ID_KEY,
    DEFAULT_ENV_ID, 
    SystemTestContextBuilder
)

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

        test_context = sys_test_context_task()
        env_id = DEFAULT_ENV_ID
        existing_cluster_name = test_context[EXISTING_CLUSTER_NAME_KEY]
        existing_cluster_subnets = test_context[EXISTING_CLUSTER_SUBNETS_KEY]


        result = {
           'new_cluster_name': f"{env_id}-cluster",
           'container_name': f"{env_id}-container",
           'family_name': f"{env_id}-task-definition",
           'asg_name': f"{env_id}-asg",
        }

        logger.info(f"<< test_context")
        return result    
    @task()
    def aws_region(prev_result:dict):
        logger.info(">> aws_region")
        result = prev_result
        result['aws_region'] = boto3.session.Session().region_name
        logger.info(f"<< aws_region") 
        return result
    @task()
    def create_cluster(prev_result:dict):
        logger.info(">> create_cluster")
        result = prev_result

        # The following does not create an ECS cluster for an unclear reason
        # create_cluster = EcsCreateClusterOperator(
        #     task_id="create_cluster",
        #     region=result['aws_region'],
        #     cluster_name=result['new_cluster_name'],
        #     wait_for_completion=True,
        # )

        # Using boto3 instead
        client = boto3.client("ecs", region_name=result['aws_region'])
        response = client.create_cluster(clusterName=result['new_cluster_name'])
        logger.info(f"<< create_cluster {json.dumps(response, indent=4)}")
        return result    
    @task()
    def await_cluster(prev_result:dict):
        logger.info(">> await_cluster")
        result = prev_result
        logger.info("<< await_cluster") 
        return result    
    @task()
    def register_task(prev_result:dict):
        logger.info(">> register_task")
        result = prev_result

        client = boto3.client("ecs", region_name=result['aws_region'])
        response = client.register_task_definition(
                containerDefinitions=[
                    {
                        "name": "AmazonSampleImage",
                        "image": "amazon/amazon-ecs-sample",
                        "cpu": 0,
                        "portMappings": [],
                        "essential": True,
                        "environment": [],
                        "mountPoints": [],
                        "volumesFrom": [],
                        "logConfiguration": {
                            "logDriver": "awslogs",
                            "options": {
                                "awslogs-group": "/ecs/AWSSampleApp",
                                "awslogs-region": result['aws_region'],
                                "awslogs-stream-prefix": "ecs"
                            }
                        }
                    }
                ],
                executionRoleArn="arn:aws:iam::548097210593:role/ecsTaskExecutionRole",
                family= "AWSSampleApp2",
                networkMode="awsvpc",
                requiresCompatibilities= [
                    "FARGATE"
                ],
                cpu= "256",
                memory= "512")
        logger.info(json.dumps(response, indent=4, default=str))        

        logger.info("<< register_task") 
        return result    
    @task()
    def await_task_definition(prev_result:dict):
        logger.info(">> await_task_definition")
        result = prev_result
        client = boto3.client("ecs", region_name=result['aws_region'])
        logger.info("<< await_task_definition") 
        return result    
    @task()
    def run_task(prev_result:dict):
        logger.info(">> run_task")
        result = prev_result
        client = boto3.client("ecs", region_name=result['aws_region'])

        ec2 = boto3.resource('ec2')
        response = client.run_task(
            taskDefinition='AWSSampleApp2',
            launchType='FARGATE',
            cluster=result['new_cluster_name'],
            platformVersion='LATEST',
            count=1,
            networkConfiguration={
                'awsvpcConfiguration': {
                    'subnets': [
                        'subnet-0477a1059cffb4665',
                    ],
                    'assignPublicIp': 'ENABLED',
                    'securityGroups': ["sg-06d507a76c7575cc3"]
                }
            }
        )
        logger.info(json.dumps(response, indent=4, default=str))

        logger.info("<< run_task") 
        return result    
    @task()
    def await_task_finish(prev_result:dict):
        logger.info(">> await_task_finish")
        result = prev_result
        client = boto3.client("ecs", region_name=result['aws_region'])

        paginator = client.get_paginator('list_tasks')
        response_iterator = paginator.paginate(
            PaginationConfig={
                'PageSize':100
            }
        )
        for each_page in response_iterator:
            for each_task in each_page['taskArns']:
                response = client.stop_task(task=each_task)
                logger.info(json.dumps(response, indent=4, default=str))

        logger.info("<< await_task_finish") 
        return result    
    @task()
    def deregister_task(prev_result:dict):
        logger.info(">> deregister_task")
        result = prev_result
        client = boto3.client("ecs", region_name=result['aws_region'])

        paginator = client.get_paginator('list_task_definitions')
        response_iterator = paginator.paginate(
            PaginationConfig={
                'PageSize':100
            }
        )
        for each_page in response_iterator:
            for each_task_definition in each_page['taskDefinitionArns']:
                response = client.deregister_task_definition(
                                taskDefinition=each_task_definition)
                logger.info(json.dumps(response, indent=4, default=str))

        logger.info("<< deregister_task") 
        return result    
    @task()
    def delete_cluster(prev_result:dict):
        logger.info(">> delete_cluster")
        result = prev_result
        client = boto3.client("ecs", region_name=result['aws_region'])

        tasks_stopped = False
        for i in range(1,10):
            tasks_stopped = True
            paginator = client.get_paginator('list_tasks')
            response_iterator = paginator.paginate(
                PaginationConfig={
                    'PageSize':100
                }
            )
            counter = 1
            for each_page in response_iterator:
                for each_task in each_page['taskArns']:
                    logger.info(each_task)
                    tasks_stopped = False
            sleep(10)

        if tasks_stopped:
            response = client.delete_cluster(cluster=result['new_cluster_name'])
            logger.info(f"<< delete_cluster {json.dumps(response, indent=4)}")
        else:
            logger.info(f"<< delete_cluster: not stopped tasks present")

        return result
    @task()
    def await_delete_cluster(prev_result:dict):
        logger.info(">> await_delete_cluster")
        result = prev_result
        client = boto3.client("ecs", region_name=result['aws_region'])
        logger.info("<< await_delete_cluster") 
        return result    

    test_context_result             = test_context()
    aws_region_result               = aws_region            (test_context_result)
    create_cluster_result           = create_cluster        (aws_region_result)
    await_cluster_result            = await_cluster         (create_cluster_result)
    register_task_result            = register_task         (await_cluster_result)
    await_task_definition_result    = await_task_definition (register_task_result)
    run_task_result                 = run_task              (await_task_definition_result)
    await_task_finish_result        = await_task_finish     (run_task_result)
    deregister_task_result          = deregister_task       (await_task_finish_result)
    delete_cluster_result           = delete_cluster        (deregister_task_result)
    await_delete_cluster_result     = await_delete_cluster  (delete_cluster_result)

ecs_deployment_test = ecs_deployment_test()
