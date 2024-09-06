from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from datetime import datetime, timedelta

# Define your SQL queries and their dependencies
sql_queries = {"sql3":"""

insert into ds_gads.company_360
SELECT
    complaints_tracking.company_name,
    company_category.category,
    complaints_tracking.complaints_30_days,
    complaints_tracking.complaints_90_days,
    complaints_tracking.complaints_180_days,
    complaints_tracking.complaints_365_days,
    complaints_tracking.complaints_timestamp
    
  FROM
    `dk4learning-433311.ds_gads.complaints_tracking` AS complaints_tracking
    LEFT OUTER JOIN `dk4learning-433311.ds_gads.company_category` AS company_category ON complaints_tracking.company_name = company_category.company_name""","sql2":"""

insert into ds_gads.complaints_tracking
SELECT
    company.company_name,
    count(CASE
      WHEN DATE_DIFF(CURRENT_DATE(), complaints.date_sent_to_company, DAY) <= 30 THEN 1
      ELSE CAST(NULL as INT64)
    END) AS complaints_30_days,
    count(CASE
      WHEN DATE_DIFF(CURRENT_DATE(), complaints.date_sent_to_company, DAY) <= 90 THEN 1
      ELSE CAST(NULL as INT64)
    END) AS complaints_90_days,
    count(CASE
      WHEN DATE_DIFF(CURRENT_DATE(), complaints.date_sent_to_company, DAY) <= 180 THEN 1
      ELSE CAST(NULL as INT64)
    END) AS complaints_180_days,
    count(CASE
      WHEN DATE_DIFF(CURRENT_DATE(), complaints.date_sent_to_company, DAY) <= 365 THEN 1
      ELSE CAST(NULL as INT64)
    END) AS complaints_365_days,
    CURRENT_DATETIME() as complaints_timestamp
  FROM
    `dk4learning-433311.ds_ent.company` AS company
    left JOIN `dk4learning-433311.ds_ent.complaints` AS complaints ON company.company_name = complaints.company_name
  GROUP BY 1""","sql4":"""
""","sql1":"""

insert into ds_gads.company_category
SELECT
    intermediate_table.company_name,
    CASE
      WHEN intermediate_table.timely_response_count / intermediate_table.total_complaints >= 0.8 THEN 'good'
      WHEN intermediate_table.timely_response_count / intermediate_table.total_complaints >= 0.5 THEN 'average'
      ELSE 'poor'
    END AS category,
    current_timestamp()
  FROM
    ds_gads.w_company_score as intermediate_table""","sql0":"""insert ds_gads.w_company_score
SELECT
    complaints.company_name,
    count(CASE
      WHEN complaints.timely_response THEN 1
      ELSE CAST(NULL as INT64)
    END) AS timely_response_count,
    count(*) AS total_complaints
  FROM
    `dk4learning-433311.ds_ent.complaints` AS complaints
  GROUP BY 1"""}

dependencies = {"sql3":["sql2","sql1"],"sql2":[],"sql1":["sql0"],"sql0":[]}


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
    "SCRUM-22_cutomer_360_dag_name_prod_dml",
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
