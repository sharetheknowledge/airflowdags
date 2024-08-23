import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
# https://airflow.apache.org/docs/apache-airflow-providers-dbt-cloud/stable/_api/airflow/providers/dbt/cloud/operators/dbt/index.html
from airflow.hooks.base_hook import BaseHook
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from datetime import datetime, timedelta
import subprocess
import pandas as pd
import io
import requests



# I just removed the function for sending email 


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    "dbt_cloud_conn_id": "dbt_cloud",
    "account_id": 70403103937328
}

dag = DAG(
    'data_processing_workflow',
    default_args=default_args,
    description='Workflow for processing and validating data',
    schedule_interval=timedelta(days=1),
)


# Configure the logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Functions

def execute_snowflake_task():
    logger.info("Executing task on snowflake.")
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    sql = "EXECUTE TASK NODE.BRONZE.SRC1_CAPEX"
    result = hook.get_pandas_df(sql)
    logger.info(f"Task returned {len(result)} rows.")
    return not result.empty






def always_fail():
    raise AirflowException('Please change this step to success to continue')





# Operators

execute_snowflake = PythonOperator(
    task_id='execute_snowflake_task',
    python_callable=execute_snowflake_task,
    dag=dag,
)

# email_user = PythonOperator(
#         task_id = 'send_email',
#         python_callable= send_email
#     )

manual_sign_off = PythonOperator(
    task_id='manual_sign_off',
    dag=dag,
    python_callable=always_fail
)

trigger_job_run1 = DbtCloudRunJobOperator(
    task_id="dbt_job",
    account_id=70403103937328,
    job_id=70403103960334,
    check_interval=10,
    timeout=300,
    dag=dag
)










# execute_snowflake  >> email_user >> manual_sign_off >> trigger_job_run1

execute_snowflake   >> manual_sign_off >> trigger_job_run1
