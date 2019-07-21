from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import Variable
from datetime import datetime, timedelta

default_args = {
    'owner': 'Valeria',
    'depends_on_past': False,
    'start_date': datetime(2019, 7, 19),
    'email': ['user@mail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    'end_date': datetime(2019, 7, 20),
}

dag = DAG(
    'testMinio', default_args=default_args, schedule_interval=timedelta(1))

t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag)

print_path_env_task = BashOperator(
    task_id='print_path_env',
    bash_command='echo $PATH',
    dag=dag)

spark_submit_task = BashOperator(
    task_id='pythonTask',
    bash_command='python /usr/local/airflow/dags/file-uploader.py',
    dag=dag,
)

t1.set_upstream(print_path_env_task)
spark_submit_task.set_upstream(t1)
