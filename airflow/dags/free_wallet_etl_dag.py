from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator


def fetch_xlsx():
    blob



default_args = {
    "depends_on_past": False,
    "email": ["caiomaia3@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    'free_wallet_etl_dag',
    default_args=default_args,
    description='The Free Wallet pipeline tha extract B3 report and transform data and save for using in Dica de Hoje spreadsheet',
    schedule_interval=timedelta(days=1),
    start_date=datetime.days_ago(2),
    catchup=False,
    tags=['dev'],
) as dag: