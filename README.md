# ⚡ PySpark & GraphFrame
**Nhánh làm việc & Nghiên cứu:** `Minh-Quân`  
**Tác giả:** Đặng Minh Quân (`@dangminhquandk`)  

> 📌 **Báo cáo Tiến độ Chi tiết:** **[BAO_CAO_TIEN_DO_SPARK.md](./BAO_CAO_TIEN_DO_SPARK.md)**

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

### Cấu trúc Mã nguồn Nhánh Minh-Quân

```text
.
├── src/
│   ├── lessons/                          # Bài học & thực hành chuẩn hóa
│   │   ├── 00_hello_spark.py             # Khởi tạo SparkSession tối ưu phần cứng
│   │   ├── wordCount.py                  # MapReduce kinh điển trên RDD
│   │   ├── day1_dataframe.py             # Transformation vs Action, Narrow vs Wide
│   │   └── self_day1_dataframe.py        # Code tự gõ thực hành và đo lường hiệu năng
│   ├── graph/                            # Động cơ đồ thị phân tán
│   │   ├── economic_graph.py             # Xây dựng đồ thị VECOM, PageRank, LPA
│   │   └── self_lpa.py                   # Triển khai thực nghiệm thuật toán LPA
│   ├── etl/
│   │   └── 01_extract_vecom.py           # Pipeline ETL trích xuất thực thể đồ thị
│   └── utils/
│       └── profiler.py                   # Bộ đo Memory RSS Growth & Wall-clock Time
├── Mini - Prj/                           # Thực nghiệm Đồ thị tương tác trên Jupyter
│   ├── mini.ipynb                        # Chu trình, Connected Components & PageRank
│   └── test.ipynb                        # Khai phá mẫu đồ thị (Motif / Frenemies)
├── LPA_Label_Propogation_Algorithm/       # Pipeline chuyên sâu thuật toán LPA
│   ├── communities.py                    # Khai phá cộng đồng
│   ├── graphx.py                         # Cấu trúc đồ thị phân tán
│   ├── read_data.py                      # Đọc và phân chia cạnh đồ thị
│   ├── results_new/                      # Kết quả benchmark xuất ra CSV
│   └── README.md                         # Báo cáo kỹ thuật thuật toán LPA
├── data/                                 # Dữ liệu mẫu (Seed Data)
│   ├── input.txt                         # Dữ liệu cho RDD WordCount
│   └── raw/accounts.csv                  # Dữ liệu giao dịch ngân hàng cho Day 1
├── Dockerfile                            # Image Spark 3.5 + GraphFrames + Jupyter
├── docker-compose.yml                    # Khởi động cụm Spark phân tán cục bộ
├── requirements.txt                      # Danh mục thư viện phụ thuộc
├── ROADMAP_4_WEEKS_GRAPHFRAME.md         # Lộ trình nghiên cứu toàn diện 4 tuần
├── BAO_CAO_TIEN_DO_SPARK.md               # Tài liệu báo cáo tiến độ chi tiết
└── README.md                             # Tài liệu tổng quan nhánh Minh-Quân
```

---

### Hướng dẫn Khởi chạy Nhanh

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
