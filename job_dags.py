from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from include.etl import extract_data, write_to_s3, read_from_s3, transform_raw_job_data, write_transformed_data_to_s3, generate_schema, create_table_from_schema, load_to_redshift 

default_args = {
    'owner': 'Chukwuka',
    'depends_on_past': False,
    'start_date': datetime(year=2024, month=2, day=16),
    'email': ['Chukwukaubani1@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG (
    'job_project',
    default_args = default_args,
    description='This job pulls jobs from an API',
    schedule_interval= '0 8 * * *', 
    catchup=False,
) as dag:
    
    get_job_data = PythonOperator(
    task_id='extract_data_task',
    python_callable=extract_data
    )

    write_to_s3_task = PythonOperator(
        task_id='write_to_s3_task',
        python_callable=write_to_s3,
        op_args=[Job_dataframe]  
    )

    read_from_s3_task = PythonOperator(
        task_id='read_from_s3_task',
        python_callable=read_from_s3
    )

    transform_raw_job_data_task = PythonOperator(
        task_id='transform_raw_job_data_task',
        python_callable=transform_raw_job_data
    )

    write_transformed_data_to_s3_task = PythonOperator(
        task_id='write_transformed_data_to_s3_task',
        python_callable=write_transformed_data_to_s3,
        op_args=[transformed_data]  
    )

    generate_schema_task = PythonOperator(
        task_id='generate_schema_task',
        python_callable=generate_schema,
        op_args=[transformed_data],  
        op_kwargs={'table_name': 'chuka1', 'exclude_columns': ['extra_column1', 'extra_column2']}  
    )

    create_table_from_schema_task = PythonOperator(
        task_id='create_table_from_schema_task',
        python_callable=create_table_from_schema,
        op_kwargs={'table_name': 'chuka1'}  
    )

    load_to_redshift_task = PythonOperator(
        task_id='load_to_redshift_task',
        python_callable=load_to_redshift,
        op_kwargs={'table_name': 'chuka1'}  
    )


get_job_data >> write_to_s3_task >> read_from_s3_task >> transform_raw_job_data_task >> write_transformed_data_to_s3_task >> generate_schema_task >> create_table_from_schema_task >> load_to_redshift_task
