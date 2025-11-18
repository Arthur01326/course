from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import clickhouse_connect
from pyspark.sql import SparkSession


def test_clickhouse():
    """Тест подключения к ClickHouse"""
    try:
        # Подключаемся к ClickHouse (стандартные credentials)
        client = clickhouse_connect.get_client(
            host='clickhouse_user',
            port=8123,
            username='default',
            password=''
        )

        # Создаем тестовую таблицу
        client.command('''
            CREATE TABLE IF NOT EXISTS test_spark_data (
                id Int32,
                name String,
                value Float64,
                created_date Date
            ) ENGINE = MergeTree()
            ORDER BY id
        ''')

        print("ClickHouse connection: SUCCESS")
        return True
    except Exception as e:
        print(f"ClickHouse connection: FAILED - {e}")
        return False


def test_pyspark_processing():
    """Тест обработки данных в PySpark"""
    try:
        spark = SparkSession.builder \
            .appName("AirflowSparkTest") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()

        # Создаем тестовые данные
        data = [(1, "Alice", 100.5), (2, "Bob", 200.3), (3, "Charlie", 150.0)]
        columns = ["id", "name", "value"]

        df = spark.createDataFrame(data, columns)
        print("PySpark DataFrame:")
        df.show()

        # Простая агрегация
        result = df.groupBy().sum("value").collect()
        print(f"Total sum: {result[0][0]}")

        spark.stop()
        print("PySpark processing: SUCCESS")
        return True
    except Exception as e:
        print(f"PySpark processing: FAILED - {e}")
        return False


with DAG(
        'test_integration',
        start_date=datetime(2024, 1, 1),
        schedule_interval=None,
        catchup=False,
        tags=['test']
) as dag:
    test_clickhouse_task = PythonOperator(
        task_id='test_clickhouse_connection',
        python_callable=test_clickhouse
    )

    test_pyspark_task = PythonOperator(
        task_id='test_pyspark_processing',
        python_callable=test_pyspark_processing
    )

    test_clickhouse_task >> test_pyspark_task