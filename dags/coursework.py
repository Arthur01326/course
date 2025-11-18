from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime
import requests
from urllib.parse import urlencode

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType

import tempfile
import os


def russian_houses_analysis():
    """Чтение файла формата csv с яндекс диска, обработка форматов данных, различные вычисления
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
        houses_df.printSchema()

        # Количество записей
        count_houses_df = houses_df.count()
        print(f"Количество записей в файле: {count_houses_df}")

        # Удаление строк с пустыми значениями
        key_columns = ["maintenance_year", "square", "region", "full_address"]
        houses_df_cleaned = houses_df.na.drop(subset=key_columns)

        # Количество записей после удаления пустых строк
        count_houses_df_cleaned = houses_df_cleaned.count()
        print(f"Количество записей после удаления пустых строк: {count_houses_df_cleaned}")


        # Преобразование данных в правильные форматы
        # Преобразование даты
        houses_df_cleaned = houses_df.withColumn(
            "maintenance_year",
            when(
                col("maintenance_year").rlike("^\\d{4}$") &
                (col("maintenance_year").cast("int") >= 1800) &
                (col("maintenance_year").cast("int") <= 2025),
                to_date(concat(col("maintenance_year"), lit("-01-01")), "yyyy-MM-dd")
            ).otherwise(None)
        ).filter(col("maintenance_year").isNotNull())  # удаляем все NULL

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

        # Проверка всех форматов данных
        houses_df_cleaned.printSchema()
        houses_df_cleaned.show(truncate=False)


        print("---Вычисления и аналитика преобразованных данных---")
        # Средний и медианный год постройки зданий
        avg_year_houses_df = houses_df_cleaned.agg(avg(year("maintenance_year")) \
                                                   .alias("avg_year"))

        median_year_houses_df = houses_df_cleaned.agg(median(year("maintenance_year")) \
                                                      .alias("median_year"))

        print(f'Средний год постройки зданий: {avg_year_houses_df.collect()[0]["avg_year"]:.0f}')
        print(f'Медианный год постройки: {median_year_houses_df.collect()[0]["median_year"]:.0f}')

        # Топ 10 регионов и городов с наибольшим кол. объектов
        top_10_region = houses_df_cleaned.groupBy(col("region")).count()
        print("Топ 10 регионов по количеству объектов")
        top_10_region.orderBy(col("count").desc()).show(10)

        top_10_locality_name = houses_df_cleaned.groupBy(col("locality_name")).count()
        print("Топ 10 городов по количеству объектов")
        top_10_locality_name.orderBy(col("count").desc()).show(10)

        # Здания с максимальной и минимальной площадью в рамках каждой области:
        # Здания с минимальной площадью
        min_squares_df = houses_df_cleaned.groupBy(col('region')).agg(min(col('square')) \
                                                                      .alias('min_square'))
        houses_min_df = houses_df_cleaned.alias('h') \
            .join(min_squares_df.alias('ms'),
                  (col('h.region') == col('ms.region')) &
                  (col('h.square') == col('ms.min_square'))) \
            .select('h.region', 'h.full_address', 'ms.min_square') \
            .orderBy('min_square')

        # Здания с максимальной площадью
        max_squares_df = houses_df_cleaned.groupBy(col('region')).agg(max(col('square')) \
                                                                      .alias('max_square'))
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

        print("--Здания с максимальной и минимальной площадью в рамках каждой области--")
        houses_analys_squares_df.show(truncate=False)


        # Количество зданий по десятилетиям:
        # Добавляем колонку с десятилетием
        houses_decade_df = houses_df_cleaned.withColumn(
            "decade",
            (floor(year(col("maintenance_year")) / 10) * 10).cast("int"))

        # Количество зданий по десятилетиям
        houses_decade_df = houses_decade_df.groupBy("decade") \
            .agg(count("*").alias("building_count")) \
            .orderBy("decade")

        print("Количество зданий по десятилетиям:")
        houses_decade_df.show(30)
        spark.stop()




























    finally:
        os.unlink(temp_file_path)

with DAG(
        dag_id='coursework',
        start_date=datetime(2024, 1, 1),
        schedule_interval='@daily',
        catchup=False,
        tags=['pyspark', 'csv', 'analysis']
) as dag:
    process_csv_simple = PythonOperator(
        task_id='process_csv_simple',
        python_callable=russian_houses_analysis
    )

