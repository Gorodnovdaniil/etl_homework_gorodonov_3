#!/usr/bin/env python3

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

def process_data():
    df = pd.read_csv('/opt/airflow/dags/data_etl.csv')

    try:
        df['noted_date_dt'] = pd.to_datetime(df['noted_date'], format='%d-%m-%Y %H:%M')
        df['noted_date'] = df['noted_date_dt'].dt.date
        print('Формат привелся в datetime')
        print(df.info())
    except:
        print('Формат НЕ привелся в datetime')

    print(df.head(10))

    try:
        Q5 = df['temp'].quantile(0.05)
        Q95 = df['temp'].quantile(0.95)
        df = df[(df['temp']>=Q5)&(df['temp']<=Q95)]
        print('По квантилями отфильтровался')
    except:
        print('По квантилями НЕ отфильтровался')

    try:
        df = df[df['out/in']=='In']
        print('По in отфильтровался')
    except:
        print('По in НЕ отфильтровался')

    try:
        df['day_of_year'] = df['noted_date_dt'].dt.dayofyear
        df_group = df.groupby('day_of_year').agg({'temp':'mean', 'noted_date_dt':'first'})
        df_group = df_group.reset_index()

        first_five = df_group.sort_values('temp', ascending=False).head(5)
        first_five = first_five[['noted_date_dt','temp']]

        last_five = df_group.sort_values('temp', ascending=True).head(5)
        last_five = last_five[['noted_date_dt','temp']]

        print('Удалось')
        print('Пять самых жарких дней в году')
        print(first_five)
        print('Пять самых холодных дней в году')
        print(last_five)

    except:
        print('Не удалось вывести топ 5 дней')

with DAG(
    dag_id='data_etl_pipeline',
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False
) as dag:
    
    task = PythonOperator(
        task_id='process_data_task',
        python_callable=process_data
    )
