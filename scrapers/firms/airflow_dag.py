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
  'FIRMS-WILDFIRE-dag',
  default_args=default_args,
  description='FIRMS WILDFIRE ALERTS',
  schedule_interval=timedelta(minutes=20),
)

dag.doc_md = __doc__

t1 = BashOperator(
  task_id='t1',
  bash_command='source /home/iflament/ioenv/bin/activate',
  dag=dag,
)

t2 = BashOperator(
  task_id='t2',
  bash_command="python3 home/iflament/oddsetl/scrapers/firms/firms.py",
  dag=dag,
)

t2.doc_md = """
#### DOCUMENTATION
"""

t1
t2

