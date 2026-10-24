## 📌 I. TỔNG QUAN 


```
                               ┌────────────────────────────────────────────────────────┐
                               │               SPARK DISTRIBUTED SYSTEM                 │               
                               └───────────────────────────┬────────────────────────────┘
                                                           │
               ┌───────────────────────────────────────────┼───────────────────────────────────────────┐
               ▼                                           ▼                                           ▼
   【 ENGINE INTERNALS & PROFILING 】           【 DISTRIBUTED GRAPH MINING 】                 【 DOCKERIZED ENVIRONMENT 】
   ─────────────────────────────────           ──────────────────────────────                 ────────────────────────────
   • Catalyst Optimizer & DAG Plans            • GraphFrames (Vertices & Edges)               • Docker Compose
   • Lazy: Transformations vs Actions          • LPA Community Detection                      • Khắc phục xung đột PySpark & JVM
   • Narrow vs Wide (Shuffle)                                                                 • Data Lakehouse: Bronze/Silver/Gold
  
```

1. ** 1: Spark:**
   - Biết được cơ chế Lazy Evaluation, lên plan chạy thông qua qua **Catalyst Optimizer** và **Tungsten Engine**.
   - Hiểu bản chất **Narrow Dependency** (xử lý in-memory pipelining tại chỗ) vs **Wide Dependency** (Shuffle Exchange phân tán).

2. ** 2:GraphFrames:**
   - **Thuật toán Lan truyền nhãn (Label Propagation Algorithm - LPA):** Triển khai phân cụm phát hiện cộng đồng tự nhiên trên dữ liệu lớn (`LPA_Label_Propogation_Algorithm/`).

3. ** 3:Dockerized Spark:**
   - Đóng gói cụm thử nghiệm qua `Dockerfile` và `docker-compose.yml` tích hợp Spark 3.5, GraphFrames JAR, Hadoop AWS và Jupyter.

---

## 🔬 II. NỘI DUNG

### 1. Kiến trúc Spark

* **Lazy Evaluation & Catalyst Optimizer:** 
  - Phân biệt giữa **Transformations** (chỉ dựng đồ thị tính toán kế hoạch thực thi logic - DAG) và **Actions** (kích hoạt Job tính toán vật lý).
  - Tránh kích hoạt Action bừa bãi (`.count()`, `.show()`) trong vòng lặp lớn để không phá vỡ khả năng tối ưu hóa pipelining của Catalyst.
* **Narrow Transformation vs Wide Transformation (Shuffle Mechanics):**
  - **Narrow (map, filter):** Thực thi tại chỗ trên từng partition trong RAM, tận dụng cache CPU, không truyền dữ liệu qua mạng.
  - **Wide (groupBy, join, distinct):** Tạo ra Shuffle Exchange, cắt đứt Stage trên DAG, phát sinh I/O đĩa và serialization overhead.

---

### 2. Khai phá Đồ thị Phân tán (Graph Mining với GraphFrames)

* **Mô hình hóa Đồ thị tri thức kinh tế (VECOM Economic Knowledge Graph - `src/graph/`):**
  - Biểu diễn mạng lưới các địa phương, sàn TMĐT và chuỗi liên kết dòng chảy kinh tế dưới dạng directed graph (Vertices $V$ & Edges $E$).
* **Thuật toán Lan truyền nhãn (Label Propagation Algorithm - LPA - `LPA_Label_Propogation_Algorithm/`):**
  - Phát hiện các cụm cộng đồng tự nhiên (Community Detection) với độ phức tạp tiệm cận gần tuyến tính:
    $$C_v^{(t+1)} = \operatorname{argmax}_c \sum_{u \in \mathcal{N}(v), C_u^{(t)} = c} w(u, v)$$
  - Chạy thực nghiệm trên cụm Spark và ghi nhận kết quả phân cụm đa partition.
* **Kiểm soát Lineage & Tránh StackOverflowError (`checkpoint()`):**
  - Các thuật toán đồ thị lặp (Iterative Graph Algorithms) làm đồ thị RDD Lineage kéo dài liên tục sau mỗi vòng lặp.
  - Giải pháp bắt buộc: Thiết lập `spark.sparkContext.setCheckpointDir()` để định kỳ cắt tỉa nhánh lineage, ghi trạng thái trung gian xuống bộ lưu trữ tin cậy, ngăn ngừa tràn ngăn xếp bộ nhớ JVM.

---

### 3. Dockerized Spark

* Đóng gói toàn bộ cụm thử nghiệm qua `Dockerfile` và `docker-compose.yml`:
  - Apache Spark 3.5.0, Python 3.11, PySpark, GraphFrames JAR (`graphframes:graphframes:0.8.3-spark3.5-s_2.12`), Hadoop AWS.
  - Tách biệt môi trường ảo nội bộ (`./venv/`), cấu hình chuẩn `SPARK_LOCAL_IP = "127.0.0.1"` và đồng bộ `PYSPARK_PYTHON = sys.executable`.

---

## 📂 III. CẤU TRÚC

```text
.
├── src/
│   ├── lessons/                          # Các bài học & thực hành chuẩn hóa
│   │   ├── wordCount.py                  # MapReduce kinh điển trên RDD
│   │   ├── dataframe_test.py             # Transformation vs Action, Narrow vs Wide
│   │   └── schema_test.py
│   ├── graph/                            # Động cơ đồ thị phân tán
│   │   ├── economic_graph.py             # Xây dựng đồ thị VECOM, PageRank, LPA
│   │   └── self_lpa.py                   # Triển khai thực nghiệm thuật toán LPA
│   └── utils/
│       └── profiler.py                   # Bộ đo Memory RSS Growth & Wall-clock Time
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
├── BAO_CAO_TIEN_DO_SPARK.md               # Tài liệu báo cáo tiến độ chi tiết
└── README.md                             
```

---
