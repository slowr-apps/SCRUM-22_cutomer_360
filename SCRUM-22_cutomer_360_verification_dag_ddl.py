from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from datetime import datetime, timedelta

# Define your SQL queries and their dependencies
sql_queries = {"sql3":"""

CREATE TABLE `dk4learning-433311.ds_gads.company_360`
(
  company_name STRING,
  category STRING,
  complaints_30_days INT64,
  complaints_90_days INT64,
  complaints_180_days INT64,
  complaints_365_days INT64,
  complaints_timestamp DATETIME
)""","sql2":"""

CREATE TABLE `dk4learning-433311.ds_gads.company_category`
(
  company_name STRING,
  category STRING,
  created_ts TIMESTAMP
)""","sql4":"""
""","sql1":"""

CREATE TABLE `dk4learning-433311.ds_gads.complaints_tracking`
(
  company_name STRING,
  complaints_30_days INT64,
  complaints_90_days INT64,
  complaints_180_days INT64,
  complaints_365_days INT64,
  complaints_timestamp DATETIME
)""","sql0":"""CREATE TABLE `dk4learning-433311.ds_gads.w_company_score`
(
  company_name STRING,
  timely_response_count INT64,
  total_complaints INT64
)"""}


# Default DAG arguments
default_args = {
    'owner': 'ammadhavan',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 1),
    'email': ['amrithamadhavan.am@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

# Create the DAG
with DAG(
    "SCRUM-22_cutomer_360_dag_name_verification_ddl",
    default_args=default_args,
    catchup=False,
    schedule_interval=None,  # Or set your desired schedule
) as dag:

    # Create tasks for each SQL query
    #'{{ dag_run.conf.get("project_id") }}'
    tasks = {}
    query_parameters =[
        {
        'name': 'paypal_batch_date',
        'parameterType': { 'type': 'STRING' },
        'parameterValue': { 'value': '{{ ds }}' }
        }
    ]
    #event dependency
    for sql_id, query in sql_queries.items():
        tasks[sql_id] = BigQueryExecuteQueryOperator(
            task_id=f"execute_{sql_id}",
            sql=query,
            use_legacy_sql=False,
            location="us",
            query_params=query_parameters
        )


    # Set task dependencies based on the dependencies dictionary
    for sql_id, deps in dependencies.items():
        for dep in deps:
            tasks[sql_id].set_upstream(tasks[dep])
