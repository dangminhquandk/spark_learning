# ⚡ PySpark & GraphFrame
> 📌 **Báo cáo Tiến độ:** **[BAO_CAO_TIEN_DO_SPARK.md](./BAO_CAO_TIEN_DO_SPARK.md)**

---

### Phân hệ Engine Internals, Profiler & GraphFrame
* **Spark Internals & Hardware Profiling:**
  * Cơ chế Lazy Evaluation, Transformations vs Actions, Narrow vs Wide shuffle mechanics (`src/lessons/`).
  * Bộ đo thời gian thực thi (Wall-clock Time) và mức tăng trưởng RAM (`src/utils/profiler.py`) tối ưu cho phần cứng máy trạm.
* **Distributed Graph Mining (GraphFrames):**
  * Mô hình hóa Đồ thị tri thức kinh tế VECOM (`src/graph/economic_graph.py`).
  * Thực nghiệm Đồ thị trên Jupyter (`Mini - Prj/mini.ipynb`, `Mini - Prj/test.ipynb`): Phát hiện chu trình (Cycle Detection), Thành phần liên thông (Connected Components) và PageRank.
  * Thuật toán **Label Propagation Algorithm (LPA)** phát hiện cộng đồng (`LPA_Label_Propogation_Algorithm/`).
  * Cơ chế cắt tỉa RDD Lineage bằng `checkpoint()` chống tràn ngăn xếp bộ nhớ JVM.
* **Môi trường & Dữ liệu thực nghiệm:**
  * Bộ dữ liệu mẫu (`data/raw/accounts.csv`, `data/input.txt`).
  * Môi trường Spark 3.5 + GraphFrames + Jupyter sẵn sàng chạy thực nghiệm (`Dockerfile`, `docker-compose.yml`).

---


### Khởi chạy 

#### 1. Khởi chạy với Môi trường Python cục bộ
```bash
# Cài đặt thư viện phụ thuộc
pip install -r requirements.txt

# Chạy kiểm thử Spark Engine & Profiling
python src/lessons/self_day1_dataframe.py

# Chạy đồ thị kinh tế & PageRank / LPA
python src/graph/economic_graph.py
```

#### 2. Khởi chạy với Cụm Docker Spark
```bash
# Khởi động cụm Spark và Jupyter
docker compose up -d

# Truy cập Spark Web UI:
# http://localhost:4040
```
