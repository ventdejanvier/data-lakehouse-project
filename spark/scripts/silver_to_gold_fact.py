import os
import sys
import argparse
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, avg, max, min, count, date_format, round as spark_round, broadcast, lit, when, expr, greatest

def get_spark_session(hive_uri, minio_ep, minio_ak, minio_sk):
    return (SparkSession.builder
            .appName("OWM Fact Table ETL")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.lakehouse.type", "hive")
            .config("spark.sql.catalog.lakehouse.uri", hive_uri)
            .config("spark.sql.defaultCatalog", "lakehouse")
            .config("spark.hadoop.fs.s3a.endpoint", minio_ep)
            .config("spark.hadoop.fs.s3a.access.key", minio_ak)
            .config("spark.hadoop.fs.s3a.secret.key", minio_sk)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .getOrCreate())

def create_fact_table(spark): 
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.gold.fact_air_quality (
            date_id INT,
            location_id INT,
            vn_aqi INT,
            dominant_pollutant STRING,
            health_implication STRING,
            is_hazardous BOOLEAN,
            data_quality_status STRING, 
            
            -- Chỉ số AQI gốc từ OWM
            avg_aqi DOUBLE,
            max_aqi INT,
            min_aqi INT,
            
            -- Các chỉ số nồng độ chất  
            avg_pm2_5 DOUBLE,
            max_pm2_5 DOUBLE,   
            avg_pm10 DOUBLE,
            max_pm10 DOUBLE,      
            
            max_1h_co DOUBLE,
            max_1h_no2 DOUBLE,
            max_1h_so2 DOUBLE,
            max_1h_o3 DOUBLE,
            max_8h_o3 DOUBLE,
            
            avg_co DOUBLE,
            avg_no2 DOUBLE,
            avg_so2 DOUBLE,
            avg_o3 DOUBLE,
            avg_no DOUBLE,
            avg_nh3 DOUBLE,
            
            record_count INT,
            measure_date DATE
        )
        USING iceberg
        LOCATION 's3a://gold/gold.db/fact_air_quality'
        PARTITIONED BY (measure_date)
    """)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--process-date", required=True)
    args = parser.parse_args()
    process_date_str = args.process_date 

    HIVE_URI = os.getenv("HIVE_METASTORE_URI", "thrift://hive-metastore:9083")
    MINIO_EP = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    MINIO_AK = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    MINIO_SK = os.getenv("MINIO_SECRET_KEY", "minioadmin")

    spark = get_spark_session(HIVE_URI, MINIO_EP, MINIO_AK, MINIO_SK)
    
    try:
        print(f"Bắt đầu xử lý bảng Fact cho ngày: {process_date_str}")
        
        process_date_obj = datetime.strptime(process_date_str, "%Y-%m-%d")
        prev_date_str = (process_date_obj - timedelta(days=1)).strftime("%Y-%m-%d")
        
        df_silver = spark.read.format("iceberg").load("lakehouse.silver.owm_air_quality")
        df_buffer = df_silver.filter(col("measure_date").isin([prev_date_str, process_date_str]))

        if df_buffer.isEmpty():
            print(f"Không tìm thấy dữ liệu nguồn cho ngày {process_date_str}. Bỏ qua.")
            return

        # Rolling O3
        window_8h = Window.orderBy("measured_at").rowsBetween(-7, 0)
        df_rolling = df_buffer \
            .withColumn("rolling_8h_o3", avg("o3").over(window_8h)) \
            .withColumn("count_8h_o3", count("o3").over(window_8h)) 

        df_target = df_rolling.filter(col("measure_date") == process_date_str)
        
        if df_target.isEmpty():
            print(f"Không có dữ liệu sau khi lọc ngày {process_date_str}.")
            return

        #AGGREGATION:
        df_agg = df_target.groupBy("measure_date").agg(
            # 1. OWM AQI
            spark_round(avg("aqi_level"), 2).alias("avg_aqi"),
            max("aqi_level").alias("max_aqi"),
            min("aqi_level").alias("min_aqi"),
            
            # 2. PM (Avg & Max)
            spark_round(avg("pm2_5"), 2).alias("avg_pm2_5"),
            max("pm2_5").alias("max_pm2_5"),   
            spark_round(avg("pm10"), 2).alias("avg_pm10"),
            max("pm10").alias("max_pm10"),      
            
            # 3. Max 1h Gases (để Tính VN_AQI)
            spark_round(max("co"), 2).alias("max_1h_co"),
            spark_round(max("no2"), 2).alias("max_1h_no2"),
            spark_round(max("so2"), 2).alias("max_1h_so2"),
            spark_round(max("o3"), 2).alias("max_1h_o3"),
            
            # 4. Max 8h O3 (Quality check)
            spark_round(max(
                when(col("count_8h_o3") >= 6, col("rolling_8h_o3")).otherwise(lit(None))
            ), 2).alias("max_8h_o3"),
            
            # 5. Avg Gases 
            spark_round(avg("co"), 2).alias("avg_co"),
            spark_round(avg("no2"), 2).alias("avg_no2"),
            spark_round(avg("so2"), 2).alias("avg_so2"),
            spark_round(avg("o3"), 2).alias("avg_o3"),
            spark_round(avg("no"), 2).alias("avg_no"),
            spark_round(avg("nh3"), 2).alias("avg_nh3"),
            
            count("*").alias("record_count")
        )

        # Breakpoints 
        bp_pm25 = [(0, 25, 0, 50), (25, 50, 51, 100), (50, 80, 101, 150), (80, 150, 151, 200), (150, 250, 201, 300), (250, 500, 301, 500)]
        bp_pm10 = [(0, 50, 0, 50), (50, 150, 51, 100), (150, 250, 101, 150), (250, 350, 151, 200), (350, 420, 201, 300), (420, 500, 301, 500)]
        bp_co = [(0, 10000, 0, 50), (10000, 30000, 51, 100), (30000, 45000, 101, 150), (45000, 60000, 151, 200), (60000, 90000, 201, 300), (90000, 120000, 301, 500)]
        bp_no2 = [(0, 100, 0, 50), (100, 200, 51, 100), (200, 700, 101, 150), (700, 1200, 151, 200), (1200, 2350, 201, 300), (2350, 3850, 301, 500)]
        bp_so2 = [(0, 125, 0, 50), (125, 350, 51, 100), (350, 550, 101, 150), (550, 800, 151, 200), (800, 1600, 201, 300), (1600, 2630, 301, 500)]
        bp_o3_1h = [(0, 160, 0, 50), (160, 200, 51, 100), (200, 300, 101, 150), (300, 400, 151, 200), (400, 800, 201, 300), (800, 1200, 301, 500)]
        bp_o3_8h = [(0, 160, 0, 50), (160, 200, 51, 100), (200, 300, 101, 150), (300, 400, 151, 200)]

        def gen_aqi_sql(col_name, breakpoints):
            sql = "CASE "
            for (bp_lo, bp_hi, i_lo, i_hi) in breakpoints:
                if bp_hi == -1:
                     sql += f" WHEN {col_name} >= {bp_lo} THEN (({i_hi} - {i_lo}) / ({bp_lo} * 0.5)) * ({col_name} - {bp_lo}) + {i_lo} "
                else:
                    sql += f" WHEN {col_name} >= {bp_lo} AND {col_name} <= {bp_hi} THEN (({i_hi} - {i_lo}) / ({bp_hi} - {bp_lo})) * ({col_name} - {bp_lo}) + {i_lo} "
            sql += " ELSE 0 END"
            return sql

        df_calc = df_agg \
            .withColumn("aqi_pm25", expr(gen_aqi_sql("avg_pm2_5", bp_pm25)).cast("int")) \
            .withColumn("aqi_pm10", expr(gen_aqi_sql("avg_pm10", bp_pm10)).cast("int")) \
            .withColumn("aqi_co", expr(gen_aqi_sql("max_1h_co", bp_co)).cast("int")) \
            .withColumn("aqi_no2", expr(gen_aqi_sql("max_1h_no2", bp_no2)).cast("int")) \
            .withColumn("aqi_so2", expr(gen_aqi_sql("max_1h_so2", bp_so2)).cast("int")) \
            .withColumn("aqi_o3_1h", expr(gen_aqi_sql("max_1h_o3", bp_o3_1h)).cast("int")) \
            .withColumn("aqi_o3_8h", expr(gen_aqi_sql("max_8h_o3", bp_o3_8h)).cast("int"))

        df_calc_o3 = df_calc.withColumn(
            "final_aqi_o3",
            when(col("max_8h_o3") > 400, col("aqi_o3_1h"))
            .otherwise(greatest(col("aqi_o3_1h"), col("aqi_o3_8h")))
        )

        df_final_aqi = df_calc_o3.withColumn(
            "vn_aqi_raw", 
            greatest(col("aqi_pm25"), col("aqi_pm10"), col("aqi_co"), col("aqi_no2"), col("aqi_so2"), col("final_aqi_o3")).cast("int")
        )

        # Quality Check
        df_quality_checked = df_final_aqi.withColumn(
            "is_valid_data",
            (col("record_count") >= 18) & 
            (col("avg_pm2_5").isNotNull() | col("avg_pm10").isNotNull())
        ).withColumn(
            "data_quality_status", 
            when(col("is_valid_data"), "Valid")
            .when(col("avg_pm2_5").isNull() & col("avg_pm10").isNull(), "Missing PM Data")
            .otherwise("Insufficient Data (<75%)")
        ).withColumn(
            "vn_aqi", 
            when(col("is_valid_data"), col("vn_aqi_raw")).otherwise(lit(None).cast("int")) 
        )

        # Labeling
        df_enriched = df_quality_checked.withColumn(
            "health_implication",
            when(col("vn_aqi").isNull(), "N/A")
            .when(col("vn_aqi") <= 50, "Good")
            .when(col("vn_aqi") <= 100, "Moderate")
            .when(col("vn_aqi") <= 150, "Poor") 
            .when(col("vn_aqi") <= 200, "Unhealthy")
            .when(col("vn_aqi") <= 300, "Very Unhealthy")
            .otherwise("Hazardous")
        ).withColumn(
            "is_hazardous",
            when(col("vn_aqi") > 200, True).otherwise(False)
        ).withColumn(
            "dominant_pollutant",
            when(col("vn_aqi").isNull(), None)
            .when(col("vn_aqi_raw") == col("aqi_pm25"), "PM2.5")
            .when(col("vn_aqi_raw") == col("aqi_pm10"), "PM10")
            .when(col("vn_aqi_raw") == col("aqi_co"), "CO")
            .when(col("vn_aqi_raw") == col("aqi_no2"), "NO2")
            .when(col("vn_aqi_raw") == col("aqi_so2"), "SO2")
            .otherwise("Ozone")
        )

        # Ghi vào Gold
        df_with_date_id = df_enriched.withColumn(
            "date_id", date_format(col("measure_date"), "yyyyMMdd").cast("int")
        )
        df_location = spark.read.format("iceberg").load("lakehouse.gold.dim_location")
        df_fact = df_with_date_id.crossJoin(broadcast(df_location).select("location_id"))

        create_fact_table(spark)

        df_fact.createOrReplaceTempView("fact_updates")
        spark.sql("""
            MERGE INTO lakehouse.gold.fact_air_quality t
            USING fact_updates s
            ON t.date_id = s.date_id AND t.location_id = s.location_id
            WHEN MATCHED THEN UPDATE SET
                t.vn_aqi = s.vn_aqi,
                t.data_quality_status = s.data_quality_status,
                t.dominant_pollutant = s.dominant_pollutant,
                t.health_implication = s.health_implication,
                t.is_hazardous = s.is_hazardous,
                t.avg_aqi = s.avg_aqi, t.max_aqi = s.max_aqi, t.min_aqi = s.min_aqi,
                t.avg_pm2_5 = s.avg_pm2_5, t.max_pm2_5 = s.max_pm2_5,
                t.avg_pm10 = s.avg_pm10, t.max_pm10 = s.max_pm10,
                t.avg_co = s.avg_co, t.max_1h_co = s.max_1h_co,
                t.avg_no2 = s.avg_no2, t.max_1h_no2 = s.max_1h_no2,
                t.avg_so2 = s.avg_so2, t.max_1h_so2 = s.max_1h_so2,
                t.avg_o3 = s.max_1h_o3, t.max_8h_o3 = s.max_8h_o3,
                t.avg_no = s.avg_no, t.avg_nh3 = s.avg_nh3,
                t.record_count = s.record_count
            WHEN NOT MATCHED THEN INSERT (
                date_id, location_id, measure_date,
                vn_aqi, dominant_pollutant, health_implication, is_hazardous,
                data_quality_status,
                avg_aqi, max_aqi, min_aqi,
                avg_pm2_5, max_pm2_5, avg_pm10, max_pm10,
                avg_co, max_1h_co, avg_no2, max_1h_no2, 
                avg_so2, max_1h_so2, avg_o3, max_8h_o3, 
                avg_no, avg_nh3,
                record_count
            ) VALUES (
                s.date_id, s.location_id, s.measure_date,
                s.vn_aqi, s.dominant_pollutant, s.health_implication, s.is_hazardous,
                s.data_quality_status,
                s.avg_aqi, s.max_aqi, s.min_aqi,
                s.avg_pm2_5, s.max_pm2_5, s.avg_pm10, s.max_pm10,
                s.avg_co, s.max_1h_co, s.avg_no2, s.max_1h_no2,
                s.avg_so2, s.max_1h_so2, s.max_1h_o3, s.max_8h_o3,
                s.avg_no, s.avg_nh3,
                s.record_count
            )
        """)
        print(f"Đã cập nhật bảng Fact cho ngày {process_date_str}")

    except Exception as e:
        print(f"Lỗi: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()