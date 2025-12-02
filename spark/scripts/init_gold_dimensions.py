import os
import shutil # <-- Thư viện hỗ trợ xóa file/thư mục
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("Export Gold to CSV Auto-Clean") \
        .master("local[*]") \
        .config("spark.driver.extraClassPath", "/opt/spark/extra-jars/hadoop-aws-3.3.4.jar:/opt/spark/extra-jars/aws-java-sdk-bundle-1.12.262.jar:/opt/spark/extra-jars/iceberg-spark-runtime-3.5_2.12-1.5.0.jar:/opt/spark/extra-jars/postgresql-42.7.3.jar") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.lakehouse.type", "hive") \
        .config("spark.sql.catalog.lakehouse.uri", "thrift://hive-metastore:9083") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    # Đường dẫn xuất file
    output_path = "/opt/spark/scripts/gold_data_export"

    # --- BƯỚC 1: DỌN DẸP THỦ CÔNG (QUAN TRỌNG) ---
    print(f"Đang kiểm tra và dọn dẹp thư mục: {output_path}")
    if os.path.exists(output_path):
        try:
            # Xóa sạch thư mục và mọi file bên trong
            shutil.rmtree(output_path)
            print("Đã xóa sạch thư mục cũ.")
        except OSError as e:
            print(f" Cảnh báo: Không thể xóa thư mục cũ. Lỗi: {e}")
    else:
        print("Thư mục chưa tồn tại, sẵn sàng ghi mới.")
    # ---------------------------------------------

    print("--- Đang đọc dữ liệu Gold... ---")
    
    df_report = spark.sql("""
        SELECT 
            f.measure_date,
            d.year, d.month, d.day_name, d.day_of_week,
            l.city_name,
            f.vn_aqi, 
            f.health_implication, 
            f.dominant_pollutant,
            f.is_hazardous,
            f.data_quality_status,
            f.avg_aqi as avg_aqi_owm, 
            f.avg_pm2_5, f.max_pm2_5,
            f.avg_pm10, f.max_pm10,
            f.avg_co, f.avg_no2, f.avg_so2, f.avg_o3,
            f.avg_no, f.avg_nh3,
            f.record_count
        FROM lakehouse.gold.fact_air_quality f
        JOIN lakehouse.gold.dim_date d ON f.date_id = d.date_id
        JOIN lakehouse.gold.dim_location l ON f.location_id = l.location_id
        ORDER BY f.measure_date
    """)

    print(f"Đang xuất file ra: {output_path}")
    
    # Ghi file mới
    df_report.coalesce(1).write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(output_path)
        
    print("Xuất file thành công")
    spark.stop()

if __name__ == "__main__":
    main()