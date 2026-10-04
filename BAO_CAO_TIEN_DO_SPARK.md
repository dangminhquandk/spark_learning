# 🚀 BÁO CÁO TIẾN ĐỘ HỌC TẬP & NGHIÊN CỨU HỆ THỐNG APACHE SPARK
**Đề tài:** Nghiên cứu Chuyên sâu Động cơ Phân tán Apache Spark & Khai phá Đồ thị Nâng cao (GraphFrames)  
**Người thực hiện:** Đặng Minh Quân (`@dangminhquandk`)  
**Nhánh làm việc:** `Minh-Quân`  
**Repository:** [https://github.com/DANGTHANH2k7/pyspark/tree/Minh-Quân](https://github.com/DANGTHANH2k7/pyspark/tree/Minh-Quân)  
**Thời gian báo cáo:** Tháng 10/2026  

---

## 📌 I. TỔNG QUAN MỤC TIÊU & ĐỊNH HƯỚNG NGHIÊN CỨU

Thay vì chỉ tiếp cận Apache Spark ở góc độ cú pháp API mức cao (High-level API), lộ trình nghiên cứu của tôi tập trung vào **Nguyên lý Hoạt động từ Đầu (First Principles)** và **Kiến trúc Thực thi Phần cứng (Hardware & Distributed Engine)**, chia thành 3 trụ cột kỹ thuật cốt lõi:

```text
                               ┌────────────────────────────────────────────────────────┐
                               │           SPARK DISTRIBUTED SYSTEM LAB                 │
                               │           Tác giả: Đặng Minh Quân                      │
                               └───────────────────────────┬────────────────────────────┘
                                                           │
               ┌───────────────────────────────────────────┼───────────────────────────────────────────┐
               ▼                                           ▼                                           ▼
   【 ENGINE INTERNALS & PROFILING 】           【 DISTRIBUTED GRAPH MINING 】                 【 DOCKERIZED ENVIRONMENT 】
   ─────────────────────────────────           ──────────────────────────────                 ────────────────────────────
   • Catalyst Optimizer & DAG Plans            • GraphFrames (Vertices & Edges)               • Docker Compose (Spark 3.5 + JARs)
   • Lazy: Transformations vs Actions          • PageRank: Random Walk & Influence            • Khắc phục xung đột PySpark & JVM
   • Narrow vs Wide (Shuffle Boundary)         • LPA Community Detection (Lan truyền nhãn)    • Tối ưu phần cứng chip Mac M3
   • Hardware Profiler (RAM RSS & Time)        • Lineage Checkpointing (Tránh StackOverflow)  • Data Lakehouse: Bronze/Silver/Gold
```

1. **Trụ cột 1: Động cơ Spark & Tối ưu hóa Phần cứng (Engine Internals & Profiling):**
   - Làm chủ cơ chế Lazy Evaluation, phân tích kế hoạch thực thi logic và vật lý qua **Catalyst Optimizer** và **Tungsten Engine**.
   - Bóc tách bản chất **Narrow Dependency** (xử lý in-memory pipelining tại chỗ) vs **Wide Dependency** (Shuffle Exchange phân tán, cắt Stage).
   - Tối ưu hóa điều phối phân vùng: khống chế `spark.sql.shuffle.partitions = 4` phù hợp với 4 nhân CPU trên chip Apple Silicon (Mac M3) thay vì 200 partition mặc định gây lãng phí tài nguyên.
   - Xây dựng bộ công cụ profiling độc lập (`src/utils/profiler.py`) giám sát đồng thời thời gian thực thi (Wall-clock Time) và mức tăng trưởng bộ nhớ RAM (Memory RSS Growth).

2. **Trụ cột 2: Khai phá Đồ thị Phân tán Nâng cao (GraphFrames & Graph Mining):**
   - **Mô hình hóa Đồ thị tri thức kinh tế (VECOM Economic Knowledge Graph):** Biểu diễn mạng lưới các địa phương, sàn TMĐT và dòng chảy kinh tế dưới dạng directed multigraph (`src/graph/economic_graph.py`).
   - **Thuật toán PageRank phân tán:** Mô phỏng Random Walk with Teleportation để xác định các hạt nhân điều phối dòng chảy kinh tế.
   - **Thuật toán Lan truyền nhãn (Label Propagation Algorithm - LPA):** Triển khai phân cụm phát hiện cộng đồng tự nhiên trên dữ liệu lớn (`LPA_Label_Propogation_Algorithm/`).
   - **Cơ chế Lineage Pruning (`checkpoint()`):** Giải quyết triệt để lỗi tràn ngăn xếp bộ nhớ JVM (**StackOverflowError**) phát sinh trong các giải thuật đồ thị lặp phân tán.
   - **Thực nghiệm tương tác trên Jupyter Notebooks (`Mini - Prj/`):** Khai phá chu trình (Cycle Detection), thành phần liên thông (Connected Components) và mẫu đồ thị (Graph Motifs).

3. **Trụ cột 3: Môi trường & Hạ tầng Triển khai Chuẩn hóa (Dockerized Spark Lab):**
   - Đóng gói toàn bộ cụm thử nghiệm qua `Dockerfile` và `docker-compose.yml` tích hợp Spark 3.5, GraphFrames JAR, Hadoop AWS và Jupyter.

---

## 🔬 II. NỘI DUNG NGHIÊN CỨU & THỰC THI CHI TIẾT

### 1. Kiến trúc Động cơ Spark & Tối ưu hóa từ Nguyên lý đầu tiên (First Principles)

* **Lazy Evaluation & Catalyst Optimizer:** 
  - Phân tách rạch ròi giữa **Transformations** (chỉ dựng đồ thị tính toán kế hoạch thực thi logic - DAG) và **Actions** (kích hoạt Job tính toán vật lý).
  - Tránh kích hoạt Action bừa bãi (`.count()`, `.show()`) trong vòng lặp lớn để không phá vỡ khả năng tối ưu hóa pipelining của Catalyst.
* **Narrow Transformation vs Wide Transformation (Shuffle Mechanics):**
  - **Narrow (map, filter):** Thực thi tại chỗ trên từng partition trong RAM, tận dụng cache CPU, không truyền dữ liệu qua mạng.
  - **Wide (groupBy, join, distinct):** Tạo ra Shuffle Exchange, cắt đứt Stage trên DAG, phát sinh I/O đĩa và serialization overhead.
  - **Tối ưu phần cứng:** Khống chế `spark.sql.shuffle.partitions = 4` phù hợp với 4 nhân CPU trên chip Apple Silicon (Mac M3), tránh tạo 200 partition rác mặc định gây lãng phí overhead điều phối.
* **Hệ thống Hardware Profiling chuyên dụng (`src/utils/profiler.py`):**
  - Giám sát đồng thời thời gian thực thi (Wall-clock Time) và mức tăng trưởng bộ nhớ RAM (Resident Set Size - RSS Memory Delta) qua thư viện `psutil` để phát hiện rò rỉ bộ nhớ hoặc bottleneck phân tán.

---

### 2. Khai phá Đồ thị Phân tán (Graph Mining với GraphFrames)

* **Mô hình hóa Đồ thị tri thức kinh tế (VECOM Economic Knowledge Graph - `src/graph/`):**
  - Biểu diễn mạng lưới các địa phương, sàn TMĐT và chuỗi liên kết dòng chảy kinh tế dưới dạng directed graph (Vertices $V$ & Edges $E$).
* **Thuật toán PageRank phân tán:**
  - Mô phỏng bước đi ngẫu nhiên có xác suất dịch chuyển (Random Walk with Teleportation $1 - \alpha = 0.15$):
    $$PR^{(t+1)}(u) = \frac{1 - \alpha}{|V|} + \alpha \sum_{v \in B_u} \frac{PR^{(t)}(v)}{L(v)}$$
  - Xác định các trung tâm hạt nhân (Core Hubs) điều phối dòng chảy kinh tế trong đồ thị.
* **Thuật toán Lan truyền nhãn (Label Propagation Algorithm - LPA - `LPA_Label_Propogation_Algorithm/`):**
  - Phát hiện các cụm cộng đồng tự nhiên (Community Detection) với độ phức tạp tiệm cận gần tuyến tính:
    $$C_v^{(t+1)} = \operatorname{argmax}_c \sum_{u \in \mathcal{N}(v), C_u^{(t)} = c} w(u, v)$$
  - Chạy thực nghiệm trên cụm Spark và ghi nhận kết quả phân cụm đa partition.
* **Kiểm soát Lineage & Tránh StackOverflowError (`checkpoint()`):**
  - Các thuật toán đồ thị lặp (Iterative Graph Algorithms) làm đồ thị RDD Lineage kéo dài liên tục sau mỗi vòng lặp.
  - Giải pháp bắt buộc: Thiết lập `spark.sparkContext.setCheckpointDir()` để định kỳ cắt tỉa nhánh lineage, ghi trạng thái trung gian xuống bộ lưu trữ tin cậy, ngăn ngừa tràn ngăn xếp bộ nhớ JVM.
* **Thực nghiệm Tương tác trên Jupyter (`Mini - Prj/`):**
  - [mini.ipynb](file:///Users/quandkh/spark_learning/Mini%20-%20Prj/mini.ipynb): Kiểm thử cấu hình an toàn cho chip Mac M3, giải thuật phát hiện chu trình (Cycle Detection), Connected Components và PageRank.
  - [test.ipynb](file:///Users/quandkh/spark_learning/Mini%20-%20Prj/test.ipynb): Khai phá các mẫu quan hệ phức tạp (Motif Finding / Frenemies).

---

### 3. Môi trường Thực nghiệm chuẩn hóa (Dockerized Spark Lab)

* Đóng gói toàn bộ cụm thử nghiệm qua `Dockerfile` và `docker-compose.yml`:
  - Apache Spark 3.5.0, Python 3.11, PySpark, GraphFrames JAR (`graphframes:graphframes:0.8.3-spark3.5-s_2.12`), Hadoop AWS.
  - Tách biệt môi trường ảo nội bộ (`./venv/`), cấu hình chuẩn `SPARK_LOCAL_IP = "127.0.0.1"` và đồng bộ `PYSPARK_PYTHON = sys.executable`.

---

## 📂 III. BẢN ĐỒ CẤU TRÚC MÃ NGUỒN NHÁNH MINH-QUÂN

Toàn bộ các tài nguyên, mã nguồn và tài liệu trên nhánh **`Minh-Quân`** được thiết kế module hóa, phục vụ trực tiếp cho lộ trình nghiên cứu trên:

```text
.
├── src/
│   ├── lessons/                          # Các bài học & thực hành chuẩn hóa
│   │   ├── 00_hello_spark.py             # Khởi tạo SparkSession chuẩn Apple Silicon
│   │   ├── wordCount.py                  # MapReduce kinh điển trên RDD
│   │   ├── day1_dataframe.py             # Transformation vs Action, Narrow vs Wide
│   │   └── self_day1_dataframe.py        # Code tự gõ thực hành và kiểm thử profiling
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

## 🎯 IV. KỊCH BẢN THUYẾT TRÌNH BÁO CÁO TIẾN ĐỘ (DÀNH CHO BẠN)

### Thời lượng gợi ý: 5 - 7 phút
| Thời gian | Phần trình bày | Nội dung trọng tâm cần làm nổi bật |
| :--- | :--- | :--- |
| **0:00 - 1:00** | **Đặt vấn đề & Mục tiêu** | • Giới thiệu định hướng: Không học cú pháp bề nổi mà đi sâu vào **Spark Engine Internals** và **Phân tán Đồ thị (GraphFrames)**.<br>• Nêu 3 trụ cột: Kiến trúc động cơ & Đo hiệu năng, Khai phá Đồ thị phân tán, Môi trường Docker Lab. |
| **1:00 - 3:00** | **Kiến trúc Engine & Profiling** | • **Lazy Evaluation:** Catalyst Optimizer tối ưu kế hoạch thực thi ra sao.<br>• **Narrow vs Wide:** Ranh giới Shuffle Exchange cắt Stage và tại sao Shuffle là chi phí đắt đỏ nhất.<br>• **Demo Profiler:** Trình bày bộ đo `profiler.py` giám sát thời gian chạy và biến thiên RAM RSS trên máy Mac M3. |
| **3:00 - 5:00** | **GraphFrames & Giải thuật Đồ thị** | • **Mô hình hóa Đồ thị:** Xây dựng đồ thị kinh tế VECOM và thực nghiệm trên Jupyter (`Mini - Prj/`).<br>• **PageRank:** Xác định nút hạt nhân trong mạng lưới.<br>• **LPA:** Thuật toán lan truyền nhãn phát hiện cộng đồng phân tán.<br>• **Kỹ thuật Checkpoint:** Giải thích lý do bắt buộc phải dùng `checkpoint()` để triệt tiêu lỗi tràn ngăn xếp JVM `StackOverflowError`. |
| **5:00 - 6:00** | **Tổng kết & Kế hoạch** | • Hoàn thành đóng gói cụm Docker Spark 3.5.<br>• Nêu kế hoạch bước tiếp theo (mở rộng quy mô dữ liệu, tích hợp Delta Lake/Iceberg). |
| **6:00 - 7:00+**| **Q&A** | Sẵn sàng trả lời phản biện từ Thầy / Mentor. |

---

## 💡 V. BỘ CÂU HỎI PHẢN BIỆN DỰ KIẾN (SOCRATIC Q&A)

### Câu hỏi 1: *"Tại sao Spark lại sử dụng cơ chế Lazy Evaluation thay vì thực thi ngay như Pandas?"*
* **Trả lời:**
  * Pandas thực thi ngay lập tức (Eager), sinh ra các DataFrame trung gian trong bộ nhớ sau mỗi dòng lệnh. Nếu làm việc với dữ liệu lớn hàng trăm Gigabyte, RAM sẽ tràn ngay lập tức và không thể tối ưu hóa xuyên suốt.
  * Spark áp dụng **Lazy Evaluation**: Các lệnh Transformation (`filter`, `select`, `join`) chỉ ghi nhận vào đồ thị **DAG (Directed Acyclic Graph)**. Khi gặp một Action (`collect`, `write`), trình tối ưu **Catalyst Optimizer** mới phân tích toàn bộ DAG: tự động gộp các phép lọc (Predicate Pushdown), chiếu chỉ các cột cần thiết (Column Pruning) và nối các thao tác vào một vòng lặp trong RAM (Pipelining), giúp hạn chế tối đa chi phí ghi đĩa và truyền tải mạng.

### Câu hỏi 2: *"Phân biệt Narrow Dependency và Wide Dependency? Tại sao phải hạn chế Wide Dependency?"*
* **Trả lời:**
  * **Narrow Dependency (Phụ thuộc hẹp):** Mỗi partition của RDD cha chỉ được dùng bởi tối đa một partition của RDD con (ví dụ: `map`, `filter`). Dữ liệu xử lý hoàn toàn tại chỗ trong RAM của Executor hiện tại (Pipelineable), không cần trao đổi dữ liệu.
  * **Wide Dependency (Phụ thuộc rộng):** Nhiều partition của RDD con phụ thuộc vào dữ liệu từ các partition của RDD cha (ví dụ: `groupByKey`, `reduceByKey`, `join`). Hiện tượng này bắt buộc Spark phải kích hoạt **Shuffle**: ghi dữ liệu tạm ra đĩa, phân phối qua mạng giữa các Executor. Shuffle là nút thắt cổ chai lớn nhất về I/O và độ trễ mạng trong hệ thống phân tán.

### Câu hỏi 3: *"Tại sao khi chạy các thuật toán lặp trên GraphFrames như PageRank hay LPA, bắt buộc phải gọi `checkpoint()`?"*
* **Trả lời:**
  * Thuật toán lặp thực hiện cập nhật trạng thái các đỉnh qua hàng chục hoặc hàng trăm vòng lặp. Mỗi vòng lặp lại tạo thêm các phép biến đổi mới nối dài đồ thị DAG RDD Lineage.
  * Nếu không cắt tỉa, cây phả hệ (Lineage Graph) quá dài sẽ khiến bộ phân tích của trình biên dịch JVM bị tràn ngăn xếp (**StackOverflowError**) và bộ nhớ RAM của Driver phình to. 
  * Lệnh `checkpoint()` sẽ ngắt đứt cây lineage dài dằng dặc, lưu trữ kết quả của vòng lặp hiện tại xuống bộ lưu trữ vật lý ổn định (Storage), và tái thiết lập một lineage mới tinh gọn từ điểm kiểm tra đó.

### Câu hỏi 4: *"Tại sao bạn lại cấu hình `spark.sql.shuffle.partitions = 4` thay vì để mặc định?"*
* **Trả lời:**
  * Giá trị mặc định của Spark là 200 partition, được thiết kế cho các cụm cluster lớn hàng trăm node.
  * Khi chạy thực nghiệm cục bộ (Local Mode) trên chip Apple Silicon (Mac M3 có 4 nhân hiệu năng cao), việc chia thành 200 partition sẽ tạo ra 200 task siêu nhỏ, khiến CPU tốn phần lớn thời gian cho việc lập lịch (Task Scheduling Overhead) và tạo ra hàng trăm file rác shuffle.
  * Thiết lập bằng đúng số nhân CPU (`4`) giúp mỗi partition tận dụng tối đa tài nguyên đa luồng và bộ nhớ đệm CPU mà không gây lãng phí điều phối.

---

## 🔮 VI. KẾ HOẠCH BƯỚC TIẾP THEO (NEXT STEPS)

1. **Mở rộng Quy mô Đồ thị:** Thử nghiệm thuật toán LPA và PageRank trên các tập dữ liệu đồ thị mạng xã hội / giao dịch quy mô hàng triệu đỉnh và cạnh.
2. **Nâng cấp tầng lưu trữ Lakehouse:** Tích hợp định dạng bảng **Delta Lake** hoặc **Apache Iceberg** để hỗ trợ giao dịch ACID, Time Travel và Schema Evolution.
3. **Triển khai Cloud Dataproc:** Đưa pipeline từ môi trường Docker cục bộ lên cụm Dataproc Serverless trên Google Cloud Platform (GCP).
