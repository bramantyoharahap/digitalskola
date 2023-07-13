import airflow
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'retry_delay': timedelta(minutes=5)
}

objDAG = DAG(
    dag_id = "dagProject5Tyo",
    default_args = default_args,
    schedule_interval = "0 1 * * *",
    dagrun_timeout = timedelta(minutes=60),
    description = "dag for project 5 by tyo",
    start_date = days_ago(1)
)

start = DummyOperator(task_id='start', dag=objDAG)

submit = SparkSubmitOperator(
    application="home/dev/airflow/spark-code/project_5_spark_code_tyo.py",
    conn_id="spark-standalone",
    task_id="spark-submit",
    dag=objDAG
)

end = DummyOperator(task_id='end', dag=objDAG)

start >> submit >> end
