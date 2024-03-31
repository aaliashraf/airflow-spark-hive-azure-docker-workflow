import os
import sys
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.test_spark_hive import spark_hive_etl


default_args = {"owner": "meali", "email_on_failure": False}

dag = DAG(dag_id="airflow_spark_hive_azure_workflow", default_args=default_args)


start_task = DummyOperator(task_id="Begin", dag=dag)

azure_datalake_task = BashOperator(
    task_id="azure_datalake_task",
    bash_command="spark-submit --jars /opt/airflow/src/jars/hadoop-azure-3.3.1.jar /opt/airflow/src/test_spark_adls.py",
    dag=dag,
)


hive_task = PythonOperator(
    task_id="hive_task",
    python_callable=spark_hive_etl,
    dag=dag,
)

end_task = DummyOperator(task_id="Ending", dag=dag)


start_task >> [azure_datalake_task, hive_task] >> end_task
