# ⚡ PySpark & Graph Mining Research Lab

Chào mừng bạn đến với kho lưu trữ nghiên cứu và thực nghiệm hệ thống **Apache Spark** phân tán của nhóm **Đặng Thành** (`@DANGTHANH2k7`) & **Đặng Minh Quân** (`@dangminhquandk`).

Dự án này ghi lại toàn bộ hành trình làm chủ Spark từ nguyên lý đầu tiên (First Principles), cấu trúc động cơ thực thi (Catalyst & Tungsten), Machine Learning phân tán (Spark MLlib) cho đến Khai phá dữ liệu đồ thị nâng cao (GraphFrames).

> 📌 **Báo cáo Tiến độ Tổng hợp cho buổi Review:** Xem tài liệu chi tiết tại **[BAO_CAO_TIEN_DO_SPARK.md](./BAO_CAO_TIEN_DO_SPARK.md)** (bao gồm phân tích kỹ thuật, kịch bản thuyết trình và bộ câu hỏi phản biện dự kiến).

---

## 👥 Phân công & Cấu trúc Dự án

### 1. Phân hệ Core DataFrame & Machine Learning (Đặng Thành phụ trách)
* **Core PySpark:** Các thao tác đọc ghi, lọc, gom nhóm, xử lý giá trị khuyết thiếu với `Imputer` (`basic.ipynb`, `filter_operation.ipynb`, `groupby_aggregateFunc.ipynb`, `null_value.ipynb`).
* **Spark MLlib Regression:**
  * Mô hình hồi quy phân tán trên bài toán Tips (`LinearRegression/tips/`).
  * Dự đoán giá nhà (`LinearRegression/house_prices/`) kết hợp phân tích toán học chuyên sâu về giải thuật quasi-Newton L-BFGS, điều chuẩn Ridge $L_2$, biến đổi logarit nhãn $\ln(y)$, và các thước đo $R^2$, RMSE, MAE.

### 2. Phân hệ Engine Internals, Profiler & GraphFrames (Đặng Minh Quân phụ trách)
* **Spark Internals & Hardware Profiling:**
  * Cơ chế Lazy Evaluation, Transformations vs Actions, Narrow vs Wide shuffle mechanics (`src/lessons/`).
  * Bộ đo thời gian thực thi (Wall-clock Time) và mức tăng trưởng RAM (`src/utils/profiler.py`) tối ưu cho phần cứng máy trạm.
* **Distributed Graph Mining (GraphFrames):**
  * Mô hình hóa Đồ thị tri thức kinh tế VECOM (`src/graph/economic_graph.py`).
  * Thuật toán **PageRank** phân tán và **Label Propagation Algorithm (LPA)** phát hiện cộng đồng (`LPA_Label_Propogation_Algorithm/`).
  * Cơ chế cắt tỉa RDD Lineage bằng `checkpoint()` chống tràn ngăn xếp bộ nhớ JVM (StackOverflowError).
* **Dockerized Spark Lab:**
  * Môi trường Spark 3.5 + GraphFrames + Jupyter sẵn sàng chạy thực nghiệm (`Dockerfile`, `docker-compose.yml`).

---

## 🛠️ Hướng dẫn Khởi chạy Nhanh

### 1. Khởi chạy với Môi trường Python cục bộ
```bash
# Cài đặt thư viện phụ thuộc
pip install -r requirements.txt

# Chạy bài học kiểm thử Spark Engine & Profiling
python src/lessons/self_day1_dataframe.py

# Chạy đồ thị kinh tế & PageRank / LPA
python src/graph/economic_graph.py
```

### 2. Khởi chạy với Cụm Docker Spark
```bash
# Khởi động cụm Spark và Jupyter
docker compose up -d

# Truy cập Spark Web UI:
# http://localhost:4040
```
