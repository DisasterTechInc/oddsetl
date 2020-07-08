#!/usr/bin/env python3
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
  'owner' : 'airflow',
  'depends_on_past' : False,
  'start_date' : days_ago(1),
  'email' : ['io@disastertech.com'],
  'email_on_failure' : True,
  'email_on_retry' : False,
  'retries' : 1,
  'retry_delay': timedelta(minutes=15),
}

dag = DAG(
  'FIRMS-SCRAPER-dag',
  default_args=default_args,
  description='NASA FIRMS SCRAPER',
  schedule_interval=timedelta(minutes=20),
)

dag.doc_md = __doc__

t1 = BashOperator(
  task_id='t2',
  bash_command="bash home/iflament/oddsetl/scrapers/firms/run.sh",
  dag=dag,
)

t1.doc_md = """
#### DOCUMENTATION
"""

