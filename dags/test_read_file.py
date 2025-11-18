from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import requests
from urllib.parse import urlencode

def read_file():
    base_url = 'https://cloud-api.yandex.net/v1/disk/public/resources/download?'
    public_key = 'https://disk.yandex.ru/d/fTiCNcXMxtHmWg'  # Ваша публичная ссылка

    final_url = base_url + urlencode(dict(public_key=public_key))
    response = requests.get(final_url)

    if response.status_code == 200:
        try:
            download_url = response.json().get('href')  # Используем .get() для безопасного доступа к 'href'
            if download_url:
                # Загружаем файл
                download_response = requests.get(download_url)

                # Отображаем содержимое файла
                file_content = download_response.content.decode('utf-8')  # Декодируем байты в строку
                print(file_content)  # Выводим содержимое файла в консоль
            else:
                print("Ключ 'href' отсутствует в ответе.")
        except ValueError:
            print(f"Невозможно декодировать JSON из ответа: {response.text}")
    else:
        print(f'Ошибка: {response.status_code}, {response.text}')

with DAG(
    dag_id='test_read_file',
    start_date=datetime(2023, 10, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:
    read_file_task = PythonOperator(
        task_id='read_file_task',
        python_callable=read_file
    )
