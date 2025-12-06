from datetime import datetime
import os
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


@dag(
    dag_id="transform_silver_owm",
    start_date=datetime(2020, 11, 25),
    schedule_interval="@daily",
    catchup=True,
    max_active_runs=3,  
    concurrency=3,
    tags=["transform", "silver", "spark", "owm", "iceberg"],
)
def transform_silver_owm_dag():
    """
    Chay Spark Job de bien doi du lieu OWM
    tu tang Bronze (JSON) sang tang Silver (bang Iceberg).
    """

    # Đọc env lúc parse để tránh lỗi khi thiếu variables/env trong Airflow.
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_access_key = os.getenv("MINIO_ROOT_USER") or os.getenv("MINIO_ACCESS_KEY") or ""
    minio_secret_key = os.getenv("MINIO_ROOT_PASSWORD") or os.getenv("MINIO_SECRET_KEY") or ""

    SparkSubmitOperator(
        task_id="run_spark_bronze_to_silver",
        conn_id="spark_conn",
        application="/opt/spark/scripts/bronze_to_silver_owm.py",
        application_args=["--process-date", "{{ ds }}"],
        env_vars={
            "MINIO_ENDPOINT": minio_endpoint,
            "MINIO_ACCESS_KEY": minio_access_key,
            "MINIO_SECRET_KEY": minio_secret_key,
            "AWS_ACCESS_KEY_ID": minio_access_key,
            "AWS_SECRET_ACCESS_KEY": minio_secret_key,
            "HIVE_METASTORE_URI": "thrift://hive-metastore:9083",
        },
        conf={
            "spark.driver.extraClassPath": "/opt/spark/extra-jars/hadoop-aws-3.3.4.jar:/opt/spark/extra-jars/iceberg-spark-runtime-3.5_2.12-1.5.0.jar:/opt/spark/extra-jars/postgresql-42.7.3.jar:/opt/spark/extra-jars/aws-java-sdk-bundle-1.12.262.jar",
            "spark.executor.extraClassPath": "/opt/spark/extra-jars/hadoop-aws-3.3.4.jar:/opt/spark/extra-jars/iceberg-spark-runtime-3.5_2.12-1.5.0.jar:/opt/spark/extra-jars/postgresql-42.7.3.jar:/opt/spark/extra-jars/aws-java-sdk-bundle-1.12.262.jar",
            # Tăng heap + overhead để tránh lỗi tràn bộ nhớ khi ghi Parquet/Iceberg
            "spark.driver.memory": "1g",
            "spark.executor.memory": "1g",
            "spark.executor.memoryOverhead": "512m",
            # Điều chỉnh sizing của warehouse và shuffle về các giá trị tối ưu cho MinIO
            "spark.sql.warehouse.dir": "s3a://silver/",
            "spark.sql.shuffle.partitions": "200",
            "spark.sql.files.maxPartitionBytes": "32m",
        },
    )


transform_silver_owm_dag()
