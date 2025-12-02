import os
import sys
import argparse
from pyspark.sql import SparkSession  
from pyspark.sql.functions import col, explode, from_unixtime, to_timestamp, lit, when, date_format, size 
from pyspark.sql.types import IntegerType, DoubleType, StringType, StructType, StructField, ArrayType, LongType


def get_spark_session(hive_metastore_uri, minio_endpoint, minio_access_key, minio_secret_key):
    """
    Khởi tạo Spark Session với các cấu hình S3A, Iceberg và Hive Metastore.
    """
    try:
        spark = (
            SparkSession.builder
            .appName("OWM Bronze to Silver (Iceberg)")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.lakehouse.type", "hive")
            .config("spark.sql.catalog.lakehouse.uri", hive_metastore_uri)
            .config("spark.sql.defaultCatalog", "lakehouse")
            
            # Cấu hình S3A
            .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
            .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            
            .getOrCreate()
        )
        print("Spark Session (Iceberg) đã được khởi tạo thành công!")
        return spark
    except Exception as e:
        print(f"Lỗi khi khởi tạo Spark Session: {e}")
        sys.exit(1)

def get_owm_schema():
    return StructType([
        StructField("coord", StructType([
            StructField("lon", DoubleType(), True),
            StructField("lat", DoubleType(), True)
        ]), True),
        StructField("list", ArrayType(StructType([
            StructField("dt", LongType(), True),
            StructField("main", StructType([
                StructField("aqi", IntegerType(), True)
            ]), True),
            StructField("components", StructType([
                StructField("co", DoubleType(), True),
                StructField("no", DoubleType(), True),
                StructField("no2", DoubleType(), True),
                StructField("o3", DoubleType(), True),
                StructField("so2", DoubleType(), True),
                StructField("pm2_5", DoubleType(), True),
                StructField("pm10", DoubleType(), True),
                StructField("nh3", DoubleType(), True)
            ]), True)
        ])), True)
    ])

def transform_and_enrich_data(df_raw):
    #Flatten
    #lấy lon, lat và explode mảng 'list'
    df_flat = df_raw.select(
        col("coord.lon").alias("lon"),
        col("coord.lat").alias("lat"),
        explode(col("list")).alias("data")
    )

    #Chọn và Biến đổi (Select & Cast Types)
    df_transformed = df_flat.select(
        col("lon").cast(DoubleType()).alias("lon"),
        col("lat").cast(DoubleType()).alias("lat"),

        # Chuyển đổi Unix timestamp (dt) sang Timestamp
        to_timestamp(from_unixtime(col("data.dt"))).alias("measured_at"),

        # AQI level
        col("data.main.aqi").cast(IntegerType()).alias("aqi_level"),

        # Lấy tất cả các thành phần (đặt alias rõ ràng để MERGE dùng được)
        col("data.components.co").cast(DoubleType()).alias("co"),
        col("data.components.no").cast(DoubleType()).alias("no"),
        col("data.components.no2").cast(DoubleType()).alias("no2"),
        col("data.components.o3").cast(DoubleType()).alias("o3"),
        col("data.components.so2").cast(DoubleType()).alias("so2"),
        col("data.components.pm2_5").cast(DoubleType()).alias("pm2_5"),
        col("data.components.pm10").cast(DoubleType()).alias("pm10"),
        col("data.components.nh3").cast(DoubleType()).alias("nh3")
    )

    # Làm giàu dữ liệu
    # Thêm cột 'aqi_label' dựa trên 'aqi_level' (theo tiêu chuẩn của OWM)
    df_enriched = df_transformed.withColumn(
        "aqi_label",
        when(col("aqi_level") == 1, lit("Good"))
        .when(col("aqi_level") == 2, lit("Fair"))
        .when(col("aqi_level") == 3, lit("Moderate"))
        .when(col("aqi_level") == 4, lit("Poor"))
        .when(col("aqi_level") == 5, lit("Very Poor"))
        .otherwise(lit("Unknown"))
        .cast(StringType())
    )
    
    # Thêm các cột ngày, tháng, năm 
    df_final = df_enriched.withColumn("measure_date", col("measured_at").cast("date")) \
                          .withColumn("measure_year", date_format(col("measure_date"), "yyyy")) \
                          .withColumn("measure_month", date_format(col("measure_date"), "MM")) \
                          .withColumn("measure_day", date_format(col("measure_date"), "dd"))

    return df_final

def create_silver_table(spark):
    # Phân vùng (partition) theo năm, tháng, ngày -> tăng tốc độ truy vấn ở tầng Gold khi lọc theo thời gian.
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.silver.owm_air_quality (
            lon DOUBLE,
            lat DOUBLE,
            measured_at TIMESTAMP,
            aqi_level INT,
            co DOUBLE,
            no DOUBLE,
            no2 DOUBLE,
            o3 DOUBLE,
            so2 DOUBLE,
            pm2_5 DOUBLE,
            pm10 DOUBLE,
            nh3 DOUBLE,
            aqi_label STRING,
            measure_date DATE,
            measure_year STRING,
            measure_month STRING,
            measure_day STRING
        )
        USING iceberg
        PARTITIONED BY (measure_year, measure_month, measure_day)
    """)
    print("Bảng 'lakehouse.silver.owm_air_quality' đã được đảm bảo tồn tại.")

def upsert_to_silver(spark, df_silver):
    """
    Sử dụng MERGE INTO (Upsert) để ghi dữ liệu vào bảng Silver, tránh trùng lặp.
    """
    # Tạo một Temporary View từ DataFrame đã biến đổi
    df_silver.createOrReplaceTempView("silver_updates")
    
    # Sử dụng MERGE INTO để cập nhật hoặc chèn
    # Khóa chính (primary key) logic là: lon, lat, và measured_at
    spark.sql("""
        MERGE INTO lakehouse.silver.owm_air_quality t
        USING silver_updates s
        ON t.lon = s.lon AND t.lat = s.lat AND t.measured_at = s.measured_at
        
        WHEN MATCHED THEN
            UPDATE SET
                t.aqi_level = s.aqi_level,
                t.co = s.co,
                t.no = s.no,
                t.no2 = s.no2,
                t.o3 = s.o3,
                t.so2 = s.so2,
                t.pm2_5 = s.pm2_5,
                t.pm10 = s.pm10,
                t.nh3 = s.nh3,
                t.aqi_label = s.aqi_label
                
        WHEN NOT MATCHED THEN
            INSERT (
                lon, lat, measured_at, aqi_level, co, no, no2, o3, so2, pm2_5, pm10, nh3, 
                aqi_label, measure_date, measure_year, measure_month, measure_day
            )
            VALUES (
                s.lon, s.lat, s.measured_at, s.aqi_level, s.co, s.no, s.no2, s.o3, s.so2, s.pm2_5, s.pm10, s.nh3, 
                s.aqi_label, s.measure_date, s.measure_year, s.measure_month, s.measure_day
            )
    """)
    print("Đã thực hiện MERGE (Upsert) vào bảng Silver thành công.")

def main():
    # 1. Parse tham số ngày từ Airflow
    parser = argparse.ArgumentParser()
    parser.add_argument("--process-date", required=True, help="Format YYYY-MM-DD")
    args = parser.parse_args()
    process_date_str = args.process_date 

    try:
        yyyy, mm, dd = process_date_str.split("-")
    except ValueError:
        print(f"Format ngày không hợp lệ: {process_date_str}")
        sys.exit(1)
 
    HIVE_URI = os.getenv("HIVE_METASTORE_URI", "thrift://hive-metastore:9083")
    MINIO_EP = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    MINIO_AK = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    MINIO_SK = os.getenv("MINIO_SECRET_KEY", "minioadmin")

    spark = None
    try:
        print(f"--- Bắt đầu xử lý cho ngày: {process_date_str} ---")
        
        # Truyền đúng tên biến HIVE_URI vừa khai báo ở trên
        spark = get_spark_session(HIVE_URI, MINIO_EP, MINIO_AK, MINIO_SK)
        
        spark.sql("CREATE DATABASE IF NOT EXISTS lakehouse.silver")
        
        # Cấu trúc: .../batch/*/{yyyy}/{mm}/{dd}*.json
        input_path = f"s3a://bronze/openweathermap/air_quality_batch/*/{yyyy}/{mm}/{dd}*.json"
        print(f"Đọc dữ liệu từ: {input_path}")

        try:
            df_raw = spark.read.option("multiline", "true").json(input_path)
        except Exception as e:
            if "Path does not exist" in str(e) or "matches no files" in str(e):
                print(f"⚠️ Không có dữ liệu (file json) cho ngày {process_date_str}. Kết thúc.")
                return
            else:
                raise e

        if df_raw.rdd.isEmpty():
            print(f"File rỗng cho ngày {process_date_str}. Bỏ qua.")
            return

        row_count = df_raw.filter(size(col("list")) > 0).count()
        if row_count == 0:
            print(f"File ngày {process_date_str} có tồn tại nhưng không có dữ liệu (list rỗng). Bỏ qua.")
            return

        # Transform & Upsert
        df_silver = transform_and_enrich_data(df_raw)
        create_silver_table(spark)
        upsert_to_silver(spark, df_silver)
        
        print(f"✅ Hoàn tất thành công cho ngày {process_date_str}.")

    except Exception as e:
        print(f"Lỗi Critical: {e}")
        sys.exit(1)
    finally:
        if spark: spark.stop()

if __name__ == "__main__":
    main()
