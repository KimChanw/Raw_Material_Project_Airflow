import requests
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

from datetime import datetime
from typing import List

def _create_table(cur, schema, table):
    """Full Refresh를 위한 테이블 drop 후 생성"""
    try:
        cur.execute('BEGIN;')
        cur.execute(f'DROP TABLE IF EXISTS {schema}.{table}')
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                date DATE NOT NULL,
                price FLOAT NOT NULL
            )
        """
        cur.execute(create_table_sql)
        cur.execute('COMMIT;')
        
    except:
        cur.execute('ROLLBACK;')
        raise

    

def _get_redshift_connection():
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    return conn.cursor()

def extract_oil_json_data_from_api(**context) -> List[dict]:
    """
    url에서 유가 데이터를 받아 json으로 반환
    
    return:
    data : 데이터가 기록된 날짜와 가격이 저장된 딕셔너리의 리스트
    """
    logging.info('Extract start')
    
    # 유가 가격 api에 사용할 api key를 airflow에 저장된 Variable로 부터 가져옴
    API_KEY = Variable.get('oil_api_key')

    crude_oil_name: str = context['params']['oil_name']
    
    # 유가 상품 이름은 brent, wti만 가능
    assert crude_oil_name in ['BRENT', 'WTI', 'brent', 'wti']
    
    # 상품 이름을 대문자화
    crude_oil_name = crude_oil_name.upper()
    
    # 유가 API 데이터를 json으로 반환
    url = f'https://www.alphavantage.co/query?function={crude_oil_name}&interval=daily&apikey={API_KEY}'

    # 엔드포인트에서 응답을 받아 옴
    # 응답을 받지 못하는 상태하면 error raise
    try:
        res = requests.get(url)
    
    except Exception as e:
        raise e
    
    # 응답으로부터 json 데이터를 받아옴
    data_points = res.json()['data']

    logging.info('Extract done')
    logging.info(data_points)
    return data_points

def transform_json_data_to_list(**context) -> List[list]:
    """
    xcom에서 앞 task에서 return한 딕셔너리 데이터를 받아 리스트에 적재
    
    return:
    날짜와 가격 데이터를 저장한 리스트
    """
    logging.info('Load start')
    
    oil_type: str = context['params']['oil_type']
    oil_type = oil_type.lower()
    
    # xcom에 push된 이전 task의 return 데이터를 가져옴 
    data_points = context['task_instance'].xcom_pull(task_ids=f'extract_{oil_type}_data')
    
    # 리스트에서 반복문을 돌려 각 날짜 별 딕셔너리에서 가격과 일자 정보를 추출함
    # 결측치에 추가할 가장 최근 가격
    latest_value = 0
    data_price_list = []
    for price_info in data_points:
        # 문자열로 저장된 가격을 실수형으로 전환
        value = price_info['value']
        
        # 가격 데이터 중 결측치가 "."으로 표현
        # 결측치는 가장 최근 유가로 설정하여 저장
        if value == '.':
            value = latest_value
        
        # 아니라면 실수로 변환
        else:
            value = float(value)
        
        data_price_list.append([price_info['date'], value])
        latest_value = value

    logging.info('Transform done')
    logging.info(data_price_list)
    
    return data_price_list

def load_oil_price_list_to_dw(**context):
    """
    redshift에 데이터를 적재하는 task
    기존 task에서 데이터와 가격을 저장한 리스트를 가져와 redshift에 적재함
    """
    oil_type: str = context['params']['oil_type']
    oil_type = oil_type.lower()
    
    schema = context['params']['schema']
    table = context['params']['table']
    
    data_price_list = context['task_instance'].xcom_pull(task_ids=f'transform_{oil_type}_data')
    
    # 트랜잭션을 걸어 full refresh, 중간에 오류 발생 시 rollback
    try:
        cur.execute("BEGIN;")
        # 테이블 생성
        _create_table(cur, schema, table)
        
        for date, price in data_price_list:
            logging.info(f"inserted - {date}, {price}")
            insert_sql = f"INSERT INTO {schema}.{table} VALUES ('{date}', {price})"
            cur.execute(insert_sql)
            
        cur.execute("COMMIT;")
    
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise
    
    logging.info('Full Refresh Done')

def join_brent_and_wti_tables(**context):
    """두 테이블을 조인하여 새로운 테이블을 생성하는 task"""
    
    # join table을 생성하기 위한 스키마와 테이블 이름
    schema = context['params']['schema']
    ctas_table = context['params']['ctas_table']
    
    # 조인할 테이블과 조인 기준이 되는 필드 이름
    left_table = context['params']['left_table']
    right_table = context['params']['right_table']
    join_column = context['params']['join_column']
    

    try:
        cur.execute('BEGIN;')
        # Full Refresh
        cur.execute(f'DROP TABLE IF EXISTS {schema}.{ctas_table}')
        logging.info("drop success")
        join_sql = f"""
        CREATE TABLE {schema}.{ctas_table} AS
            SELECT l.date as date, l.price as wti, r.price as brent
            FROM {left_table} l
            JOIN {right_table} r ON l.{join_column} = r.{join_column};
        """
        cur.execute(join_sql)
        cur.execute('COMMIT;')
        
    except:
        cur.execute('ROLLBACK;')
        raise
    
    logging.info('Join Tables Done')

with DAG(
    dag_id='crude_oils_ETL_dag',
    start_date=datetime(2023,1,1),
    schedule='0 12 * * *', # GST 기준 매일 오후 12시 update
    max_active_runs=1, # 해당 dag의 동시 실행 횟수는 1번으로 제한
    catchup=False,
) as dags:
    
    # airflow에 저장한 redshift 접속 정보
    cur = _get_redshift_connection()
    
    wti_price_extract_task = PythonOperator(
        task_id='extract_wti_data',
        python_callable=extract_oil_json_data_from_api,
        params={
            'oil_name' : 'WTI'
        }
    )
    brent_price_extract_task = PythonOperator(
        task_id='extract_brent_data',
        python_callable=extract_oil_json_data_from_api,
        params={
            'oil_name' : 'BRENT'
        }
    )

    wti_price_transform_task = PythonOperator(
        task_id='transform_wti_data',
        python_callable=transform_json_data_to_list,
        params={
            'oil_type' : 'wti'
        }
    )
    
    brent_price_transform_task = PythonOperator(
        task_id='transform_brent_data',
        python_callable=transform_json_data_to_list,
        params={
            'oil_type' : 'brent'
        }
    )
    
    wti_price_load_task = PythonOperator(
        task_id='load_wti_data',
        python_callable=load_oil_price_list_to_dw,
        params={
            'schema' : Variable.get('schema'),
            'table' : 'WtiPriceTable',
            'oil_type' : 'wti'
        }
    )
    
    brent_price_load_task = PythonOperator(
        task_id='load_brent_data',
        python_callable=load_oil_price_list_to_dw,
        params={
            'schema' : Variable.get('schema'),
            'table' : 'BrentPriceTable',
            'oil_type' : 'brent'
        }
    )
    
    join_task = PythonOperator(
        task_id='join_wti_and_brent',
        python_callable=join_brent_and_wti_tables,
        params={
            'schema' : Variable.get('oil_schema'),
            'ctas_table' : 'WtiBrentJoinTable',            
            'left_table' : 'WtiPriceTable',
            'right_table' : 'BrentPriceTable',
            'join_column' : 'date'
        }
    )
    
    wti_price_extract_task >> wti_price_transform_task >> wti_price_load_task
    brent_price_extract_task >> brent_price_transform_task >> brent_price_load_task
    
    [wti_price_load_task, brent_price_load_task] >> join_task