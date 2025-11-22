from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, DoubleType, DateType

import clickhouse_connect

import logging
import requests
from urllib.parse import urlencode
from datetime import datetime
import tempfile
import os
import pandas as pd

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def russian_houses_analysis(**kwargs):
    """Чтение файла формата csv с Яндекс диска, обработка форматов данных, различные вычисления
    и аналитика, загрузка данных в clickhouse"""

    # Ссылка для скачивания
    base_url = 'https://cloud-api.yandex.net/v1/disk/public/resources/download?'
    public_key = 'https://disk.yandex.ru/d/fTiCNcXMxtHmWg'

    final_url = base_url + urlencode(dict(public_key=public_key))
    response = requests.get(final_url)
    download_url = response.json().get('href')

    # Скачиваем файл
    with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.csv') as tmp_file:
        download_response = requests.get(download_url)
        tmp_file.write(download_response.content)
        temp_file_path = tmp_file.name

    # Работа pyspark
    try:
        # Создаем SparkSession
        spark = SparkSession.builder \
            .appName("houses_analysis") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()

        # Загружаем с правильной кодировкой
        houses_df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("encoding", "utf-16") \
            .option("sep", ",") \
            .option("multiLine", "true") \
            .csv(temp_file_path)

        # Проверка формата
        logger.info('Проверка схемы данных')
        houses_df.printSchema()

        # Количество записей
        count_houses_df = houses_df.count()
        logger.info(f"Количество записей в файле: {count_houses_df}")

        # Удаление строк с пустыми значениями
        key_columns = ["maintenance_year", "square", "region", "full_address"]
        houses_df_cleaned = houses_df.na.drop(subset=key_columns)

        # Количество записей после удаления пустых строк
        count_houses_df_cleaned = houses_df_cleaned.count()
        logger.info(f"Количество записей после удаления пустых строк: {count_houses_df_cleaned}")


        # Преобразование данных в правильные форматы
        # Преобразование даты с валидацией диапазона
        houses_df_cleaned = houses_df_cleaned.withColumn(
            "maintenance_year_int",
            when(
                col("maintenance_year").rlike("^\\d{4}$") &
                (col("maintenance_year").cast("int") >= 1800) &
                (col("maintenance_year").cast("int") <= 2025),
                col("maintenance_year").cast("int")
            ).otherwise(1900)
        )

        # Преобразование колонки "square" как double
        houses_df_cleaned = houses_df_cleaned.withColumn(
            "square",
            regexp_replace(col("square"), " ", "").cast("double"))

        # Преобразование колонки "population" как integer
        houses_df_cleaned = houses_df_cleaned.withColumn(
            "population",
            regexp_replace(col("population"), " ", "").cast("integer"))

        # Преобразование колонки "communal_service_id" в формат double
        houses_df_cleaned = houses_df_cleaned.withColumn(
            "communal_service_id",
            col("communal_service_id").cast("double"))

        # Приведение house_id к integer
        houses_df_cleaned = houses_df_cleaned.withColumn(
            "house_id",
            col("house_id").cast("integer")
        )

        # Проверка всех форматов данных
        logger.info("Схема данных после преобразований:")
        houses_df_cleaned.printSchema()
        logger.info("Первые 5 строк:")
        houses_df_cleaned.show(5, truncate=False)

        logger.info("---Вычисления и аналитика преобразованных данных---")

        # Средний и медианный год постройки зданий
        avg_year_houses_df = houses_df_cleaned.agg(avg("maintenance_year_int").alias("avg_year"))
        median_year_houses_df = houses_df_cleaned.agg(
            expr("percentile_approx(maintenance_year_int, 0.5)").alias("median_year"))

        avg_year = avg_year_houses_df.collect()[0]["avg_year"]
        median_year = median_year_houses_df.collect()[0]["median_year"]

        logger.info(f'Средний год постройки зданий: {avg_year:.0f}')
        logger.info(f'Медианный год постройки: {median_year:.0f}')

        # Топ 10 регионов и городов с наибольшим кол. объектов
        top_10_region = houses_df_cleaned.groupBy(col("region")).count()
        logger.info("Топ 10 регионов по количеству объектов")
        top_10_region.orderBy(col("count").desc()).show(10)

        top_10_locality_name = houses_df_cleaned.groupBy(col("locality_name")).count()
        logger.info("Топ 10 городов по количеству объектов")
        top_10_locality_name.orderBy(col("count").desc()).show(10)

        # Здания с максимальной и минимальной площадью в рамках каждой области
        # Сначала вычисляем минимальные площади
        min_squares_df = houses_df_cleaned.groupBy(col('region')).agg(min(col('square')).alias('min_square'))
        houses_min_df = houses_df_cleaned.alias('h') \
            .join(min_squares_df.alias('ms'),
                  (col('h.region') == col('ms.region')) &
                  (col('h.square') == col('ms.min_square'))) \
            .select('h.region', 'h.full_address', 'ms.min_square') \
            .orderBy('min_square')

        # Вычисление максимальных площадей
        max_squares_df = houses_df_cleaned.groupBy(col('region')).agg(max(col('square')).alias('max_square'))
        houses_max_df = houses_df_cleaned.alias('h') \
            .join(max_squares_df.alias('max_s'),
                  (col('h.region') == col('max_s.region')) &
                  (col('h.square') == col('max_s.max_square'))) \
            .select('h.region', 'h.full_address', 'max_s.max_square') \
            .orderBy('max_square')

        # Join двух вычислений в один DataFrame
        houses_analys_squares_df = houses_min_df.alias('h_min') \
            .join(houses_max_df.alias('h_max'),
                  (col('h_min.region') == col('h_max.region')), 'full') \
            .select(col('h_min.region'),
                    col('h_min.min_square'),
                    col('h_min.full_address').alias('min_square_address'),
                    col('h_max.max_square'),
                    col('h_max.full_address').alias('max_square_address'))

        logger.info("--Здания с максимальной и минимальной площадью в рамках каждой области--")
        houses_analys_squares_df.show(truncate=False)

        # Количество зданий по десятилетиям
        houses_decade_df = houses_df_cleaned.withColumn(
            "decade",
            (floor(col("maintenance_year_int") / 10) * 10).cast("int"))

        houses_decade_df = houses_decade_df.groupBy("decade") \
            .agg(count("*").alias("building_count")) \
            .orderBy("decade")

        logger.info("Количество зданий по десятилетиям:")
        houses_decade_df.show(30)



        # Подготовка данных для ClickHouse
        final_df = houses_df_cleaned.select(
            col("house_id"),
            col("latitude"),
            col("longitude"),
            col("maintenance_year_int").alias("maintenance_year"),
            col("square"),
            col("population"),
            col("region"),
            col("locality_name"),
            col("address"),
            col("full_address"),
            col("communal_service_id"),
            col("description")
        )

        # Проверка данных перед загрузкой
        logger.info('Финальная схема данных: ')
        final_df.printSchema()
        logger.info('Пример 5 строк: ')
        final_df.show(5, truncate=False)

        # Конвертируем в Pandas
        pandas_df = final_df.toPandas()
        logger.info(f'Размер Pandas DataFrame: {len(pandas_df)} строк')

        # Преобразование колонки 'maintenance_year' в диапазон дат 1970-2025 в строковый формат
        pandas_df = pandas_df[
            (pandas_df['maintenance_year'] >= 1970) &
            (pandas_df['maintenance_year'] <= 2025)
            ]

        # Обрабатываем NaN значения - гарантируем, что ключевые колонки не будут NULL
        pandas_df['house_id'] = pandas_df['house_id'].fillna(0).astype('int32')
        pandas_df['maintenance_year'] = pandas_df['maintenance_year'].fillna('').astype('string')
        pandas_df['population'] = pandas_df['population'].fillna(0).astype('int32')
        pandas_df['square'] = pandas_df['square'].fillna(0).astype('float64')
        pandas_df['communal_service_id'] = pandas_df['communal_service_id'].fillna(0).astype('float64')
        pandas_df['latitude'] = pandas_df['latitude'].fillna(0).astype('float64')
        pandas_df['longitude'] = pandas_df['longitude'].fillna(0).astype('float64')

        pandas_df['region'] = pandas_df['region'].fillna('')
        pandas_df['locality_name'] = pandas_df['locality_name'].fillna('')
        pandas_df['address'] = pandas_df['address'].fillna('')
        pandas_df['full_address'] = pandas_df['full_address'].fillna('')
        pandas_df['description'] = pandas_df['description'].fillna('')



        # Подключение к ClickHouse
        logger.info('Подключаемся к ClickHouse')
        client = clickhouse_connect.get_client(
            host='host.docker.internal',
            port=8123,
            username='default',
            password=''
        )
        logger.info('Подключение установлено')

        # Создаем таблицу в ClickHouse
        create_table = """
        CREATE OR REPLACE TABLE russian_houses
        (   
            house_id UInt32,
            latitude Float64,
            longitude Float64,
            maintenance_year String,
            square Float64,
            population UInt32,
            region String,      
            locality_name String,
            address String,
            full_address String,
            communal_service_id Float64,
            description String
        )       
        ENGINE = MergeTree() 
        ORDER BY (house_id); 
        """

        # Создаем таблицу
        client.command(create_table)
        logger.info('Таблица создана')

        # Загружаем данные
        logger.info('Начинаем загрузку данных')

        client.insert_df('russian_houses',pandas_df)

        logger.info(f'Загружено {len(pandas_df)} записей в таблицу')

        # После загрузки проверяем общее количество
        count_result = client.query("SELECT COUNT(*) FROM russian_houses")
        logger.info(f"Всего записей в ClickHouse: {count_result.result_rows[0][0]}")

        # Проверка данных загруженных данных
        result = client.query("""
                              SELECT house_id, maintenance_year, region
                              FROM russian_houses
                              LIMIT 5 
                              """)
        logger.info(f'Пример загруженных данных:')
        for row in result.result_rows:
            logger.info(f' house_id: {row[0]}, maintenance_year: {row[1]}, region: {row[2]}')

        # SQL скрипт топ 25 домов, у которых площадь больше 60 кв.м
        top_25_houses_square = client.query("""
                                            SELECT house_id, square, description
                                            FROM russian_houses
                                            WHERE square > 60
                                            ORDER BY square
                                            LIMIT 25
                                            """)
        logger.info('Топ 25 домов, у которых площадь больше 60 кв.м :')
        for row in top_25_houses_square.result_rows:
            logger.info(f'house_id: {row[0]}, square: {row[1]}, description: {row[2]}')



    except Exception as e:
        logger.error(f'Ошибка при обработки данных: {e}')
        logger.exception('Детали ошибки:')
        raise

    finally:
        spark.stop()
        logger.info('Spark сессия остановлена')

        os.unlink(temp_file_path)
        logger.info('Временный файл удален')


# Определение DAG
with DAG(
        dag_id='coursework',
        start_date=datetime(2024, 1, 1),
        schedule_interval='@daily',
        catchup=False,
        tags=['pyspark', 'csv', 'analysis', 'clickhouse']
) as dag:
    process_csv_simple = PythonOperator(
        task_id='process_csv_simple',
        python_callable=russian_houses_analysis,
        provide_context=True
    )












