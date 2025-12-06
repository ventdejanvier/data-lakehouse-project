import os
import requests
import json
from datetime import datetime, timedelta, timezone
from io import BytesIO

from airflow.decorators import dag, task
from airflow.models import Variable
from minio import Minio
from urllib.parse import urlparse

# --- Config ---
# Lấy config từ Airflow Variables  
try:
    MINIO_URL = Variable.get("MINIO_URL")
    MINIO_ACCESS_KEY = Variable.get("MINIO_ACCESS_KEY")
    MINIO_SECRET_KEY = Variable.get("MINIO_SECRET_KEY")
    OWM_API_KEY = Variable.get("OWM_API_KEY")
except Exception as e:
    print(f"Lỗi: Không thể lấy Variables từ Airflow. Hãy chắc chắn bạn đã tạo chúng. {e}")
    # Gán giá trị mặc định để DAG có thể parse
    MINIO_URL, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, OWM_API_KEY = "", "", "", ""

if not OWM_API_KEY:
    OWM_API_KEY = os.getenv("OWM_API_KEY", "")

BRONZE_BUCKET = "bronze" 

CITY_INFO = {"city": "Ho Chi Minh City", "lat": 10.762622, "lon": 106.660172} #tọa độ của TP HCM

def _parse_minio_endpoint(raw_url):
    """Normalize MINIO_URL and return (hostname, port)."""
    if not raw_url:
        raise ValueError("Airflow Variable 'MINIO_URL' chua co gia tri.")
    normalized = raw_url if "://" in raw_url else f"http://{raw_url}"
    parsed = urlparse(normalized)
    if not parsed.hostname:
        raise ValueError(f"MINIO_URL khong hop le: '{raw_url}'.")
    if parsed.port is None:
        raise ValueError("MINIO_URL phai bao gom cong (vi du: 'minio:9000').")
    return parsed.hostname, parsed.port

# --- DAG Definition ---
@dag(
    dag_id="ingest_batch_owm",  
    start_date=datetime(2020, 11, 25), 
    schedule_interval="@daily",
    #Cho phép Airflow chạy bù
    catchup=True,
    max_active_runs=3, 
    tags=["ingest", "batch", "bronze", "owm", "air-quality"],
)
def ingest_batch_owm_dag():
    """
    (BATCH) Cào dữ liệu lịch sử chất lượng không khí từ OpenWeatherMap.
    Mỗi DAG Run sẽ cào dữ liệu cho 1 ngày, dựa trên execution_date.
    """

    def get_minio_client():
        endpoint_host, endpoint_port = _parse_minio_endpoint(MINIO_URL)

        return Minio(
            f"{endpoint_host}:{endpoint_port}",
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False 
        )

    @task(
        #Gán task vào Pool đã tạo
        pool="owm_pool",
        retries=3, # Tự động retry nếu API lỗi
    )
    def fetch_historical_owm_by_day(city_info: dict, execution_date: str):
        """
        Lấy dữ liệu OWM cho 1 ngày (dựa trên execution_date).
        API OWM /history yêu cầu 'start' và 'end' là Unix timestamps.
        """
        lat = city_info["lat"]
        lon = city_info["lon"]

        # Chuyển execution_date (ví dụ: '2024-01-01') thành timestamp
        # execution_date là 00:00:00 của ngày đó
        start_dt = datetime.fromisoformat(execution_date).replace(tzinfo=timezone.utc)
        start_timestamp = int(start_dt.timestamp())

        #  Tạo timestamp cho cuối ngày (23:59:59)
        end_dt = start_dt + timedelta(days=1) - timedelta(seconds=1)
        end_timestamp = int(end_dt.timestamp())

        api_url = (
            "https://api.openweathermap.org/data/2.5/air_pollution/history"
            f"?lat={lat}&lon={lon}"
            f"&start={start_timestamp}&end={end_timestamp}"
            f"&appid={OWM_API_KEY}"
        )

        print(f"BATCH (OWM): Cào ngày: {start_dt.strftime('%Y-%m-%d')}")

        try:
            response = requests.get(api_url, timeout=30)
            response.raise_for_status() # Dừng nếu có lỗi HTTP
            data = response.json()

            client = get_minio_client()
            if not client.bucket_exists(BRONZE_BUCKET):
                print(f"Bucket '{BRONZE_BUCKET}' không tồn tại. Đang tạo...")
                client.make_bucket(BRONZE_BUCKET)
            else:
                print(f"Đã tìm thấy bucket '{BRONZE_BUCKET}'.")


            json_data = json.dumps(data, indent=4).encode("utf-8")

            # Lưu file vào MinIO
            city_slug = city_info["city"].lower().replace(" ", "_")
            object_name = (
                f"openweathermap/air_quality_batch"
                f"/{city_slug}"
                f"/{start_dt.year}/{start_dt.month:02d}/{start_dt.day:02d}.json"
            )

            client.put_object(
                BRONZE_BUCKET, object_name, BytesIO(json_data), len(json_data),
                content_type="application/json"
            )
            print(f"Đã lưu (lịch sử OWM): {object_name}")
            return object_name

        except Exception as e:
            print(f"Lỗi khi cào ngày {start_dt.strftime('%Y-%m-%d')}: {e}")
            raise

    # Truyền execution_date (dưới dạng '{{ ds }}') vào task
    fetch_historical_owm_by_day(city_info=CITY_INFO, execution_date="{{ ds }}")

ingest_batch_owm_dag()
