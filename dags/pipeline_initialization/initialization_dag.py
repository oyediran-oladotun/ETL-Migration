import pendulum
from airflow.sdk import DAG
from datetime import datetime, timedelta
from airflow.timetables.trigger import MultipleCronTriggerTimetable
from airflow.providers.standard.operators.python import PythonOperator


from pipeline_initialization.scripts.pipeline_init import (
    run_container_1,
    run_container_2,
    run_container_3,
    run_container_4,
)

default_args = {
    "owner": "data_engineer",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email": ["dotun.oyediran@movam.ng"],
    "email_on_failure": True,
    "email_on_retry": False,
}

with DAG(
    dag_id="pipeline_initialization_dags",
    start_date=pendulum.datetime(2026, 1, 20, tz="Africa/Lagos"),
    schedule= "30 1 * * *",                      
    catchup=False,
    default_args=default_args, 
    max_active_runs=1,
) as dag:

    initialize_pipeline = PythonOperator(
        task_id="initialize_pipeline_task",
        python_callable=run_container_1,
    )

    fetch_visibility_loss = PythonOperator(
        task_id="fetch_visibility_loss_task",
        python_callable=run_container_2,
    )

    visibility_loss_analysis = PythonOperator(
        task_id="visibility_loss_analysis_task",
        python_callable=run_container_3,
    )

    tbl_visibility_safety_report = PythonOperator(
        task_id="tbl_visibility_safety_report_task",
        python_callable=run_container_4,
    )

    (
        initialize_pipeline
        >> fetch_visibility_loss
        >> visibility_loss_analysis
        >> tbl_visibility_safety_report
    )