from datetime import datetime
import os
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

@dag(
    dag_id="transform_gold_fact",
    start_date=datetime(2020, 11, 25),
    schedule_interval="@daily",
    catchup=True,
    max_active_runs=3, # Chạy 3 ngày cùng lúc
    concurrency=3,
    tags=["gold", "fact", "star-schema"],
)
def transform_gold_fact_dag():
    
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_access_key = os.getenv("MINIO_ROOT_USER") or "minioadmin"
    minio_secret_key = os.getenv("MINIO_ROOT_PASSWORD") or "minioadmin"

    SparkSubmitOperator(
        task_id="run_spark_fact_daily",
        conn_id="spark_conn",
        application="/opt/spark/scripts/silver_to_gold_fact.py",
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
            # Cấu hình tiết kiệm tài nguyên
            "spark.driver.memory": "512m",
            "spark.executor.memory": "512m",
            "spark.executor.memoryOverhead": "384m",
            "spark.sql.warehouse.dir": "s3a://gold/",
        },
    )

transform_gold_fact_dag()