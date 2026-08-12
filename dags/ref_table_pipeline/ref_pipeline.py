import pendulum
from airflow.sdk import DAG
from datetime import datetime, timedelta
from airflow.timetables.trigger import MultipleCronTriggerTimetable
from airflow.providers.standard.operators.python import PythonOperator

from ref_table_pipeline.scripts.ref_table_script import run_company_vehicle_new
from ref_table_pipeline.scripts.ref_table_script import run_company_vehicle_inactive
from ref_table_pipeline.scripts.ref_table_script import run_device_vehicle_map
from ref_table_pipeline.scripts.ref_table_script import run_point_of_interest
from ref_table_pipeline.scripts.ref_table_script import run_devices
from ref_table_pipeline.scripts.ref_table_script import run_company_info
from ref_table_pipeline.scripts.ref_table_script import run_company_vehicles
from ref_table_pipeline.scripts.ref_table_script import run_3pl
from ref_table_pipeline.scripts.ref_table_script import run_company_drivers


local_tz = pendulum.timezone("Africa/Lagos")

default_args = {
    "owner": "data_engineer",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email": ["dotun.oyediran@movam.ng"], # Add your email address(es) here
    "email_on_failure": True,            # Trigger email if a task fails
    "email_on_retry": False,          # Do not trigger email on retry
}

with DAG(
    dag_id="reference_table_pipeline_dags",
    start_date=pendulum.datetime(2026, 1, 20, tz="Africa/Lagos"),
    schedule=MultipleCronTriggerTimetable(
        "0 6 * * *",              # 6:00 AM
        "30 18 * * *",            # 6:30 PM - 18 is from the count of hours from 12am
        timezone="Africa/Lagos",
    ),    
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
) as dag:

    company_vehicle_new_etl = PythonOperator(
        task_id="company_vehicle_new_task",
        python_callable= run_company_vehicle_new,
)

    company_vehicle_inactive_etl = PythonOperator(
        task_id="company_vehicle_inactive_task",
        python_callable= run_company_vehicle_inactive,
)

    device_vehicle_map_etl = PythonOperator(
        task_id="device_vehicle_map_task",
        python_callable= run_device_vehicle_map,
)

    point_of_interest_etl = PythonOperator(
        task_id="point_of_interest_task",
        python_callable= run_point_of_interest,
)

    devices_etl = PythonOperator(
        task_id="devices_task",
        python_callable= run_devices,
)

    company_info_etl = PythonOperator(
        task_id="company_info_task",
        python_callable= run_company_info,
)

    company_vehicles_etl = PythonOperator(
        task_id="company_vehicles_task",
        python_callable= run_company_vehicles,
)

    third_party_logistics_etl = PythonOperator(
        task_id="3pl_task",
        python_callable= run_3pl,
)
    
    company_drivers_etl = PythonOperator(
        task_id="company_drivers_task",
        python_callable= run_company_drivers,
)

company_vehicle_new_etl >> company_vehicle_inactive_etl >> device_vehicle_map_etl >> point_of_interest_etl >> devices_etl >> company_info_etl >> company_vehicles_etl >> third_party_logistics_etl >> company_drivers_etl

    
