import os
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("Export Gold to CSV Full") \
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

    print("Đang xuất dữ liệu tầng Gold")
    
    # Query lấy tất cả các cột cần thiết
    df_report = spark.sql("""
        SELECT 
            f.measure_date,
            d.year, d.month, d.day_name, d.day_of_week,
            l.city_name,
            -- Chỉ số VN
            f.vn_aqi, 
            f.health_implication, 
            f.dominant_pollutant,
            f.is_hazardous,
            f.data_quality_status,
            
            -- Chỉ số OWM (để so sánh)
            f.avg_aqi as avg_aqi_owm, 
            
            -- Nồng độ chi tiết (Quan trọng: Phải có max_pm2_5)
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

    output_path = "/opt/spark/scripts/gold_data_export"
    
    df_report.coalesce(1).write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(output_path)
        
    print("Đã xuất file thành công")
    spark.stop()

if __name__ == "__main__":
    main()