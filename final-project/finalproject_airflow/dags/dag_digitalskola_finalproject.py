from datetime import datetime
import airflow
from airflow import DAG
from airflow.models import Variable, Connection
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

from modules.covid_scraper import CovidScraper
from modules.db_connector import DatabaseConnector
from modules.transformer import Transformer
import logging

mysql_conn = Connection.get_connection_from_secrets('MYSQL')
mysql_engine = DatabaseConnector().connect_mysql(
    user=mysql_conn.login,
    host=mysql_conn.host,
    password=mysql_conn.password,
    db=mysql_conn.schema,
    port=mysql_conn.port
)

pg_conn = Connection.get_connection_from_secrets('POSTGRES')
pg_engine = DatabaseConnector().connect_postgres(
    user=pg_conn.login,
    host=pg_conn.host,
    password=pg_conn.password,
    db=pg_conn.schema,
    port=pg_conn.port
)

transformer = Transformer(mysql_engine=mysql_engine, pg_engine=pg_engine)

def fetch_covid_data():
    url = Variable.get('URL_COVID_TRACKER')
    fetcher = CovidScraper(url)
    df = fetcher.fetch_data()

    try:
        drop_table_covid_jabar = 'DROP TABLE IF EXISTS covid_jabar'
        mysql_engine.execute(drop_table_covid_jabar)
        df.to_sql(con=mysql_engine,name='covid_jabar',index=False)
        logging.info('DATA INSERTED SUCCESS TO MYSQL')
    except Exception as ex:
        logging.error(ex)

def insert_data_to_dim_province():
    transformer.insert_data_to_dim_province()

def insert_data_to_dim_district():
    transformer.insert_data_to_dim_district()

def insert_data_to_dim_case():
    transformer.insert_data_to_dim_case()

def insert_data_to_district_daily():
    transformer.insert_data_to_district_daily()

def insert_data_to_province_daily():
    transformer.insert_data_to_province_daily()


objDAG = DAG(
    dag_id = "dag_digitalskola_finalproject",
    schedule_interval = "0 1 * * *",
    description = "dag for final project",
    start_date = datetime(2023,1,1)
)

step1 = PythonOperator(
    task_id = 'fetch_covid_data',
    python_callable = fetch_covid_data,
    dag = objDAG
)

step2 = PythonOperator(
    task_id = 'insert_data_to_dim_province',
    python_callable = insert_data_to_dim_province,
    dag = objDAG
)

step3 = PythonOperator(
    task_id = 'insert_data_to_dim_district',
    python_callable = insert_data_to_dim_district,
    dag = objDAG
)

step4 = PythonOperator(
    task_id = 'insert_data_to_dim_case',
    python_callable = insert_data_to_dim_case,
    dag = objDAG
)

step5 = PythonOperator(
    task_id = 'insert_data_to_province_daily',
    python_callable = insert_data_to_province_daily,
    dag = objDAG
)

step6 = PythonOperator(
    task_id = 'insert_data_to_district_daily',
    python_callable = insert_data_to_district_daily,
    dag = objDAG
)

step1 >> step2 >> step3 >> step4 >> step5 >> step6
