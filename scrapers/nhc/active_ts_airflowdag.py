#!/usr/bin/env python3
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
  'owner' : 'airflow',
  'depends_on_past' : False,
  'start_date' : days_ago(0),
  'email' : ['io@disastertech.com'],
  'email_on_failure' : True,
  'email_on_retry' : False,
  'retries' : 1,
  'retry_delay': timedelta(minutes=5),
}

dag = DAG(
  'NHC-dag',
  default_args=default_args,
  description='Active tropical storms',
  schedule_interval=timedelta(minutes=360),
)

dag.doc_md = __doc__

t1 = BashOperator(
  task_id='t1',
  bash_command='source /home/iflament/ioenv/bin/activate',
  dag=dag,
)

t2 = BashOperator(
  task_id='t2',
  bash_command="python3 home/iflament/oddsetl/scrapers/nhc/nhc.py --storms_to_get='' --odds_container='nhc' --year='2020' --scrapetype='active'",
  dag=dag,
)

t2.doc_md = """
#### DOCUMENTATION
The NHC scraper fetchs all active tropical storms in 2020 from: https://www.nhc.noaa.gov/gis/archive_forecast.php?year=2020
Please see /home/iflament/code/nhc_params.yaml for configurations. If upload is set to "active", only active storms will be imported for all active storms. If upload is set to all, ALL (!) historical data for each storm will be imported for the requested year.
"""

t1
t2
