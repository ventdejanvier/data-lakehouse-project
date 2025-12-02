# Sử dụng image Airflow chính thức
FROM apache/airflow:2.8.0-python3.8

# Lấy UID/GID từ file .env lúc build
ARG AIRFLOW_UID=50000
ARG AIRFLOW_GID=0

# Chuyển sang user root để cài đặt
USER root

# --- SỬA LỖI: CÀI ĐẶT JAVA 17 ---
# Debian 12 (Bookworm) dùng openjdk-17 làm mặc định
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME trỏ đến bản Java 17
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
# ---------------------------------------------

# Chuyển về user airflow  
USER ${AIRFLOW_UID}:${AIRFLOW_GID}
 
COPY ./docker/airflow/requirements.txt /
# ---------------------------------

# Chạy lệnh pip install để cài đặt các thư viện
# (Bao gồm cả 'pyspark' bạn đã thêm ở bước trước)
RUN pip install --no-cache-dir -r /requirements.txt