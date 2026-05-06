import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from extract import extract_data
from transform import transform_data
from load import performance, indicators, ranking


def run_extract_data(**context):
    logical_date = context.get("logical_date")
    if logical_date is None:
        logical_date = pendulum.now("America/Sao_Paulo")

    start_date = logical_date.subtract(days=1).to_date_string()
    end_date = logical_date.to_date_string()
    extract_data(start_date=start_date, end_date=end_date)


with DAG(
    dag_id="financial-pipeline",
    description="Data pipeline from yfinance",
    schedule="0 12 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["pipeline", "yfinance"]
) as dag:

    extract_data_task = PythonOperator(
        task_id="extract_data",
        python_callable=run_extract_data,
    )

    transform_data_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data
    )

    performance_task = PythonOperator(
        task_id="performance",
        python_callable=performance,
    )

    indicators_task = PythonOperator(
        task_id="indicators",
        python_callable=indicators
    )

    ranking_task = PythonOperator(
        task_id="ranking",
        python_callable=ranking
    )

    extract_data_task >> transform_data_task >> [performance_task, indicators_task, ranking_task]