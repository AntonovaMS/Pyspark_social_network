import airflow
from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os
from datetime import date, datetime

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

default_args = {
'owner': 'airflow',
'start_date':datetime(2020, 1, 1),
}

dag_spark = DAG(
dag_id = "datalake_project",
default_args=default_args,
schedule_interval=None,
)

users_geo = SparkSubmitOperator(
task_id='zadanie_2',
dag=dag_spark,
application ='/lessons/zadanie_2.py' ,
conn_id= 'yarn_spark',
application_args = ["2022-03-31", "31","/user/master/data/events", "/user/antonovams/data/geo.csv","/user/antonovams/data/analytics"],
conf={
"spark.driver.maxResultSize": "20g"
},
executor_cores = 1,
executor_memory = '1g'
)

zone_statistics = SparkSubmitOperator(
task_id='zadanie_3',
dag=dag_spark,
application ='/lessons/zadanie_3.py' ,
conn_id= 'yarn_spark',
application_args = ["2022-03-31", "31", "/user/master/data/events", "/user/antonovams/data/geo.csv","/user/antonovams/data/analytics"],
conf={
"spark.driver.maxResultSize": "20g"
},
executor_cores = 1,
executor_memory = '1g'
)

friends_recomendations = SparkSubmitOperator(
task_id='zadanie_4',
dag=dag_spark,
application ='/lessons/zadanie_4.py' ,
conn_id= 'yarn_spark',
application_args = ["2022-03-31", "31", "/user/master/data/events", "/user/antonovams/data/geo.csv","/user/antonovams/data/analytics"],
conf={
"spark.driver.maxResultSize": "20g"
},
executor_cores = 1,
executor_memory = '1g'
)

users_geo >> zone_statistics >> friends_recomendations