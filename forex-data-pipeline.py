from email.policy import default
from tabnanny import verbose
from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.email import EmailOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

import csv
import requests
import json
#default arguments:
default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

def download_rates():
    BASE_URL = "https://gist.githubusercontent.com/marclamberti/f45f872dea4dfd3eaa015a4a1af4b39b/raw/"
    ENDPOINTS = {
        'USD': 'api_forex_exchange_usd.json',
        'EUR': 'api_forex_exchange_eur.json'
    }
    with open('/opt/airflow/dags/files/forex_currencies.csv') as forex_currencies:
        reader = csv.DictReader(forex_currencies, delimiter=';')
        for idx, row in enumerate(reader):
            base = row['base']
            with_pairs = row['with_pairs'].split(' ')
            indata = requests.get(f"{BASE_URL}{ENDPOINTS[base]}").json()
            outdata = {'base': base, 'rates': {}, 'last_update': indata['date']}
            for pair in with_pairs:
                outdata['rates'][pair] = indata['rates'][pair]
            with open('/opt/airflow/dags/files/forex_rates.json', 'a') as outfile:
                json.dump(outdata, outfile)
                outfile.write('\n')

def _get_message() -> str:
    return "Hi from forex_data_pipeline2"

 # For creating a DAG: 
    # 1st arg, a unique name for datapipeline. 2nd arg, startdate for pipeline to start running
    # 3rd arg, schedule interval. 4th arg, assign the default args in data pipeline
    #catchup ~ prevents all non triggered to run between current date and starting date
with DAG("forex_data_pipeline2", start_date=datetime(2022, 10, 6), 
    schedule_interval ="@daily", default_args=default_args, catchup=False) as dag:

    # 1st Task: Checking if forex rates are available
    # Http Sensor checks every 60seconds if the forex rates are available
    is_forex_rates_available = HttpSensor(
        task_id="is_forex_rates_available", #task id must be unique, it is required for every operator
        http_conn_id="forex_api", #created in airflow dashboard
        endpoint="marclamberti/f45f872dea4dfd3eaa015a4a1af4b39b",
        response_check=lambda response: "rates" in response.text, # if rates is in response.text then we get the JSON as expected
        # Last 2 arguments are for all sensors
        # poke_interval is frequency at which it will check if url is avail. 5s here
        # After 20s, the sensor is running it will timeout, if not defined then sensor wll run for 7 days by default
        poke_interval=5,
        timeout=20
    )

    # 2nd Task: Checks if forex currency *FILE* is available
    is_forex_currencies_file_available = FileSensor(
        task_id="is_forex_currencies_file_available",
        fs_conn_id="forex_path", #created in airflow dashboard
        filepath="forex_currencies.csv", 
        poke_interval=5,
        timeout=20
    )
    
    # 3rd Task: Download forex rates with Python
    downloading_rates = PythonOperator(
        task_id="downloading_rates",
        python_callable=download_rates
    )
   
   # 4th task: Saving Rates in HDFS
   # using """""" triple quotes to write code on multiple lines, here && means we want to execute another line so we add '\' to it
   # bash command 1st line: creates new folder, 'forex' at root directory 
    saving_rates = BashOperator(
        task_id="saving_rates",
        bash_command="""
            hdfs dfs -mkdir -p /forex && \
            hdfs dfs -put -f $AIRFLOW_HOME/dags/files/forex_rates.json /forex
        """
        #here forex will be parent folder of forex_rates.json
    )

    # 5th task: Creating Hive table to store forex rates from HDFS
    creating_forex_rates_table = HiveOperator(
        task_id="creating_forex_rates_table",
        hive_cli_conn_id="hive_conn",
        hql="""
            CREATE EXTERNAL TABLE IF NOT EXISTS forex_rates(
                base STRING,
                last_update DATE,
                eur DOUBLE,
                usd DOUBLE,
                nzd DOUBLE,
                gbp DOUBLE,
                jpy DOUBLE,
                cad DOUBLE
                )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
        """
    )

    # 6th Task: Processing Forex rates with Spark 
    forex_processing = SparkSubmitOperator(
        task_id="forex_processing",
        application= "/opt/airflow/dags/scripts/forex_processing.py",
        conn_id="spark_conn",
        verbose=False   
    )
    
    # 7th Task: Sending Email Notification
    send_email_notification = EmailOperator(
        task_id="send_email_notification",
        to="airflow_course@yopmail.com",
        subject="forex_data_pipeline2",
        html_content="<h3>forex_data_pipeline2</h3>"
    )
    
    # 8th Task: Sending Slack Notification
    send_slack_notification = SlackWebhookOperator(
        task_id="send_slack_notification",
        http_conn_id="slack_conn",
        message=_get_message(),
        channel="#monitoring"
    )

    #is_forex_rates_available will be executed first and then is_forex_currencies_file_available
    #is_forex_rates_available.set_downstream(is_forex_currencies_file_available)

    # both of these have same meaning. Here is_forex_currencies_file_available will be executed after is_forex_rates_available
    #is_forex_currencies_file_available.set_upstream(is_forex_rates_available)

    # >> means set_downstream
    # << means set_upstream

    # when not using one line to show all dependencies, then we must add last dependency of line at start of next line like downloading_rates 
    is_forex_rates_available >> is_forex_currencies_file_available >> downloading_rates 
    downloading_rates >> saving_rates >> creating_forex_rates_table >> forex_processing 
    forex_processing >> send_email_notification >> send_slack_notification