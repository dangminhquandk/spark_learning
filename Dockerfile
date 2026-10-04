# 1. Sử dụng hệ điều hành Linux Debian nhẹ có sẵn Python 3.10
FROM python:3.10-slim-bookworm

# 2. Cài đặt OpenJDK 17 (Yêu cầu bắt buộc để Spark chạy)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        curl \
        procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 3. Thiết lập biến môi trường JAVA_HOME tự động thích ứng với cả Apple Silicon (ARM64) và Intel (AMD64)
RUN ln -s /usr/lib/jvm/java-17-openjdk-* /usr/lib/jvm/java-17-openjdk
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk
ENV PATH=$JAVA_HOME/bin:$PATH

# 4. Thư mục làm việc bên trong container
WORKDIR /app

# 5. Cài đặt thư viện Python (PySpark, GraphFrames, Psutil)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 6. Mở cổng 4040 (Spark Web UI) và 8888 (Jupyter Lab)
EXPOSE 4040 8888

# Lệnh mặc định khi container khởi động: mở sẵn terminal bash
CMD ["bash"]
