import requests
import logging

from airflow import DAG
from datetime import datetime
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def get_redshift_connection():
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    return conn.cursor()

def _create_gold_table(cur, schema, table):
    try:
        cur.execute('BEGIN;')
        cur.execute(f'DROP TABLE IF EXISTS {schema}.{table}')
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                date DATE NOT NULL,
                usd_am FLOAT,
                usd_pm FLOAT,
                gbp_am FLOAT,
                gbp_pm FLOAT,
                euro_am FLOAT,
                euro_pm FLOAT
            )
        """
        cur.execute(create_table_sql)
        cur.execute('COMMIT;')
        
    except:
        cur.execute('ROLLBACK;')
        raise

def _create_silver_table(cur, schema, table):
    try:
        cur.execute('BEGIN;')
        cur.execute(f'DROP TABLE IF EXISTS {schema}.{table}')
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                date DATE NOT NULL,
                usd FLOAT,
                gbp FLOAT,
                euro FLOAT
            )
        """
        cur.execute(create_table_sql)
        cur.execute('COMMIT;')
        
    except:
        cur.execute('ROLLBACK;')
        raise


def gold_extract(**context):
    """
    금 데이터 API 호출
    """
    API_KEY = Variable.get('nasdaq_api_key')
    request_url = f'https://data.nasdaq.com/api/v3/datasets/LBMA/GOLD.json?api_key={API_KEY}'
    gold_res = requests.get(request_url)
    context['ti'].xcom_push(key='gold', value=gold_res.json()['dataset']['data'])


def silver_extract(**context):
    """
    은 데이터 API 호출
    """
    API_KEY = Variable.get('nasdaq_api_key')
    request_url = f'https://data.nasdaq.com/api/v3/datasets/LBMA/SILVER.json?api_key={API_KEY}'
    silver_res = requests.get(request_url)
    context['ti'].xcom_push(key='silver', value=silver_res.json()['dataset']['data'])


def gold_load(**context):
    """
    금 데이터 INSERT
    """
    schema = context['params']['schema']
    table = context['params']['table']
    
    data = context['ti'].xcom_pull(key='gold')
    try:
        cur.execute('BEGIN;')
        _create_gold_table(cur, schema, table)
        for d in data:
            logging.info(d)
            for idx in range(1, 7):
                if d[idx] is None:
                    d[idx] = 'Null'
            
            sql = f"INSERT INTO {schema}.{table} VALUES ('{d[0]}', {d[1]}, {d[2]}, {d[3]}, {d[4]}, {d[5]}, {d[6]})"
            cur.execute(sql)

        cur.execute("COMMIT;")
        
    except:
        cur.execute("ROLLBACK;")
        raise

def silver_load(**context):
    """
    은 데이터 INSERT
    """
    schema = context['params']['schema']
    table = context['params']['table']
    
    data = context['ti'].xcom_pull(key='silver')
    try:
        cur.execute('BEGIN;')
        _create_silver_table(cur, schema, table)
        for d in data:
            logging.info(d)
            for idx in range(1, 4):
                if d[idx] is None:
                    d[idx] = 'Null'
                    
            sql = f"INSERT INTO {schema}.{table} VALUES ('{d[0]}', {d[1]}, {d[2]}, {d[3]})"
            cur.execute(sql)

        cur.execute("COMMIT;")
        
    except:
        cur.execute("ROLLBACK;")
        raise

cur = get_redshift_connection()

dag = DAG(
        dag_id='material_prices',
        start_date=datetime(2022, 1, 1),  # 날짜가 미래인 경우 실행이 안됨
        schedule='0 5 * * MON-FRI   ',  # 적당히 조절
        max_active_runs=1,
        catchup=False
)

gold_extract = PythonOperator(
        task_id='gold_extract',
        python_callable=gold_extract,
        provide_context=True,
        dag=dag,
    )

silver_extract = PythonOperator(
        task_id='silver_extract',
        python_callable=silver_extract,
        provide_context=True,
        dag=dag
    )


gold_load = PythonOperator(
        task_id='gold_load',
        python_callable=gold_load,
        provide_context=True,
        dag=dag,
        params={
            'schema' : Variable.get('schema'),
            'table' : 'GoldPriceTable'
        }
    )

silver_load = PythonOperator(
        task_id='silver_load',
        python_callable=silver_load,
        provide_context=True,
        dag=dag,
        params={
            'schema' : Variable.get('schema'),
            'table' : 'SilverPriceTable'
        }
    )


gold_extract >> gold_load
silver_extract >> silver_load
