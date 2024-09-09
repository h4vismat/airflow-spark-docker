from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow'
}

# Define the DAG
with DAG(
    'Pipeline',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2023, 9, 7),
    catchup=False,
) as dag:

    # Define the Spark job using the SparkSubmitOperator
    run_raw = SparkSubmitOperator(
        task_id='run_raw',
        application='/opt/airflow/dags/spark_jobs/raw.py',  # Path to your Spark job
        conn_id='spark_test',  # Connection ID for Spark
        application_args=['10'],  # Arguments to pass to the Spark job (e.g., number of iterations)
        verbose=True,
    )

    run_refined = SparkSubmitOperator(
        task_id='run_refined',
        application='/opt/airflow/dags/spark_jobs/refined.py',  # Path to your Spark job
        conn_id='spark_test',  # Connection ID for Spark
        application_args=['10'],  # Arguments to pass to the Spark job (e.g., number of iterations)
        verbose=True,
    )


    run_trusted = SparkSubmitOperator(
        task_id='run_trusted',
        application='/opt/airflow/dags/spark_jobs/trusted.py',  # Path to your Spark job
        conn_id='spark_test',  # Connection ID for Spark
        application_args=['10'],  # Arguments to pass to the Spark job (e.g., number of iterations)
        verbose=True,
    )
    
    run_raw >> run_refined >> run_trusted
