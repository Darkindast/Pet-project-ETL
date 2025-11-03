from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from sqlalchemy import create_engine
import pandas as pd
import os

default_args = {
    'owner': '@moskovtsev',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 26),
    'email': ['https://t.me/dark'],
    'schedule_interval': "@hourly",
}

CSV_PATH = os.path.join(os.path.dirname(__file__), "global_housing.csv")

def _prepare_analytics_table(**kwargs):
    ti = kwargs['ti']
    df = pd.read_csv(CSV_PATH)
    
    # Новые колонки для анализа
    df['Price_to_Rent_Ratio'] = df['House Price Index'] / df['Rent Index']
    df['Adjusted_Affordability'] = df['Affordability Ratio'] * (1 + df['Inflation Rate (%)']/100)
    
    # Пример фильтрации: только США после 2015 года
    df_filtered = df[(df['Country'] == 'USA') & (df['Year'] >= 2015)]
    
    tmp_path = '/tmp/analytics_table.csv'
    df_filtered.to_csv(tmp_path, index=False)
    
    ti.xcom_push(key='analytics_path', value=tmp_path)
    print(f"Создана аналитическая таблица: {len(df_filtered)} строк")

def _load_analytics_to_postgres(**kwargs):
    ti = kwargs['ti']
    tmp_path = ti.xcom_pull(key='analytics_path', task_ids='prepare_analytics_table')
    df = pd.read_csv(tmp_path)
    
    engine = create_engine('postgresql://user:user@host.docker.internal:5431/user')
    df.to_sql('analytics_table', engine, if_exists='replace', schema='public', index=False)
    
    print(f"В Postgres загружено {len(df)} строк аналитической таблицы")

dag = DAG(
    dag_id="analytics_table_",
    default_args=default_args,
    catchup=False
)

create_table_psql = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='psql_connection',
    sql="""
    DROP TABLE IF EXISTS analytics_table;
    CREATE TABLE analytics_table (
        Country text,
        Year int,
        House_Price_Index numeric,
        Rent_Index numeric,
        Affordability_Ratio numeric,
        Mortgage_Rate numeric,
        Inflation_Rate numeric,
        GDP_Growth numeric,
        Population_Growth numeric,
        Urbanization_Rate numeric,
        Construction_Index numeric,
        Price_to_Rent_Ratio numeric,
        Adjusted_Affordability numeric
    );
    """,
    dag=dag
)

prepare_analytics_table = PythonOperator(
    task_id='prepare_analytics_table',
    python_callable=_prepare_analytics_table,
    dag=dag
)

load_analytics_to_postgres = PythonOperator(
    task_id='load_analytics_to_postgres',
    python_callable=_load_analytics_to_postgres,
    dag=dag
)

create_table_psql >> prepare_analytics_table >> load_analytics_to_postgres
