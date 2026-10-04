# 🚀 BÁO CÁO TIẾN ĐỘ HỌC TẬP & NGHIÊN CỨU HỆ THỐNG APACHE SPARK
**Dự án:** Nghiên cứu & Ứng dụng Apache Spark: Từ Nền tảng Engine, MLlib đến Phân tán Đồ thị (GraphFrames)  
**Nhóm thực hiện:** Đặng Thành (`@DANGTHANH2k7`) & Đặng Minh Quân (`@dangminhquandk`)  
**Repository:** [https://github.com/DANGTHANH2k7/pyspark](https://github.com/DANGTHANH2k7/pyspark)  
**Thời gian báo cáo:** Tháng 10/2026  

---

## 📌 I. TỔNG QUAN LỘ TRÌNH VÀ PHÂN CÔNG HỌC TẬP

Để tiếp cận Apache Spark một cách toàn diện và có chiều sâu hệ thống, nhóm chia lộ trình học tập thành hai trụ cột song song bổ trợ lẫn nhau:
* **Trụ cột 1 (Đặng Thành phụ trách):** Nền tảng DataFrame API, Feature Engineering phân tán và Mô hình học máy phân tán (**Spark MLlib**).
* **Trụ cột 2 (Đặng Minh Quân phụ trách):** Kiến trúc lõi Spark Engine, Cơ chế Shuffle & Tối ưu hóa phần cứng, Môi trường Docker Lab và Khai phá dữ liệu đồ thị nâng cao (**GraphFrames & Graph Mining**).

```text
                               ┌─────────────────────────────────────────────────────┐
                               │             APACHE SPARK SYSTEM LAB                 │
                               └──────────────────────────┬──────────────────────────┘
                                                          │
                    ┌─────────────────────────────────────┴─────────────────────────────────────┐
                    ▼                                                                           ▼
      【 ĐẶNG THÀNH 】                                                            【 ĐẶNG MINH QUÂN 】
  Core DataFrame & Machine Learning (MLlib)                                  Engine Internals & Graph Mining (GraphFrames)
  ──────────────────────────────────────────                                 ────────────────────────────────────────────
  • PySpark Core (Filter, GroupBy, Impute)                                   • Spark Internals (Catalyst, Tungsten, DAG)
  • Feature Pipeline (StringIndexer, OHE)                                    • Lazy Evaluation (Transformations vs Actions)
  • MLlib Linear Regression (House Prices, Tips)                             • Shuffle Mechanics & Stage Boundaries
  • Toán tối ưu phân tán: Quasi-Newton L-BFGS                                • Hardware Profiling (Execution Time, Memory)
  • Đánh giá mô hình: R², RMSE, MAE, Log Transform                           • GraphFrames: PageRank & LPA Community Detection
                                                                             • Lineage Pruning (Checkpointing for Iterative DAGs)
                                                                             • Dockerized Multi-service Spark Environment
```

---

## 🔬 II. NỘI DUNG NGHIÊN CỨU & THỰC THI CHI TIẾT

### 1. Phân hệ của Đặng Thành: Core DataFrame & Machine Learning (MLlib)

#### 1.1. Thao tác dữ liệu nền tảng trên PySpark DataFrame
* **Xử lý giá trị khuyết (`null_value.ipynb`):** Triển khai `Imputer` song song hóa trên các partition để bù khuyết theo giá trị trung vị (`median`), xử lý các kiểu dữ liệu dị thường mà không gây lệch schema.
* **Lọc và Biến đổi dữ liệu (`basic.ipynb`, `filter_operation.ipynb`):** Sử dụng các biểu thức điều kiện phân tán, so khớp chuỗi regex, ép kiểu an toàn ANSI SQL (`try_cast`).
* **Tổng hợp và Gom nhóm (`groupby_aggregateFunc.ipynb`):** Thực thi gom nhóm song song qua cơ chế Map-side Aggregation để tối ưu lưu lượng shuffle qua mạng.

#### 1.2. Kỹ thuật đặc trưng & Học máy phân tán (MLlib Regression)
* **Dataset thực nghiệm:** Tips dataset (`LinearRegression/tips/`) và House Prices dataset (`LinearRegression/house_prices/`).
* **Feature Engineering Pipeline:**
  - `StringIndexer`: Mã hóa các biến định danh phân loại theo tần suất xuất hiện.
  - `OneHotEncoder(dropLast=True)`: Chuyển đổi sang `SparseVector`, triệt tiêu bẫy đa cộng tuyến (Dummy Variable Trap).
  - `VectorAssembler`: Gom các biến số và vector thành vector không gian trạng thái $\mathbf{x} \in \mathbb{R}^{127}$.
* **Thuật toán tối ưu hóa phân tán L-BFGS:**
  - Giải bài toán hồi quy Ridge có điều chuẩn $L_2$:
    $$\min_{\mathbf{w}, b} \frac{1}{2N} \sum_{i=1}^N (y^{(i)} - (\mathbf{w}^T \mathbf{x}^{(i)} + b))^2 + \frac{\lambda}{2} \|\mathbf{w}\|_2^2$$
  - Spark MLlib tính toán gradient song song trên các worker node, xấp xỉ ma trận nghịch đảo Hessian $H^{-1}$ mà không tốn dung lượng RAM lưu trữ toàn bộ ma trận $127 \times 127$.
* **Phân tích toán học nâng cao:**
  - Ứng dụng biến đổi logarit nhãn $\ln(y)$ để nén phân phối lệch phải, chuyển bài toán từ tối thiểu hóa sai số tiền tuyệt đối sang tối thiểu hóa sai số phần trăm tương đối.
  - Phân tích Bất đẳng thức Jensen: Khi giải mã hàm mũ $\hat{y} = \exp(\hat{y}_{\text{log}})$, mô hình thực chất ước lượng Trung vị có điều kiện (Conditional Median), giải thích lý do vì sao MAE giảm ngoạn mục $10.5\%$ (từ $2.768$M xuống $2.479$M INR).

---

### 2. Phân hệ của Đặng Minh Quân: Engine Internals & Graph Analytics (GraphFrames)

#### 2.1. Động cơ thực thi Spark & Tối ưu hóa từ Nguyên lý đầu tiên (First Principles)
* **Lazy Evaluation & Catalyst Optimizer:** 
  - Phân tách rạch ròi giữa **Transformations** (chỉ dựng đồ thị tính toán kế hoạch thực thi logic - DAG) và **Actions** (kích hoạt Job tính toán vật lý).
  - Tránh kích hoạt Action bừa bãi (`.count()`, `.show()`) trong vòng lặp lớn để không phá vỡ khả năng tối ưu hóa pipelining của Catalyst.
* **Narrow Transformation vs Wide Transformation (Shuffle Mechanics):**
  - **Narrow (map, filter):** Thực thi tại chỗ trên từng partition trong RAM, tận dụng cache CPU, không truyền dữ liệu qua mạng.
  - **Wide (groupBy, join, distinct):** Tạo ra Shuffle Exchange, cắt đứt Stage trên DAG, phát sinh I/O đĩa và serialization overhead.
  - **Tối ưu phần cứng:** Khống chế `spark.sql.shuffle.partitions = 4` phù hợp với 4 nhân CPU trên chip Apple Silicon (Mac M3), tránh tạo 200 partition rác mặc định gây lãng phí overhead điều phối.
* **Hệ thống Hardware Profiling chuyên dụng (`src/utils/profiler.py`):**
  - Giám sát đồng thời thời gian thực thi (Wall-clock Time) và mức tăng trưởng bộ nhớ RAM (Resident Set Size - RSS Memory Delta) qua thư viện `psutil` để phát hiện rò rỉ bộ nhớ hoặc bottleneck phân tán.

#### 2.2. Khai phá Đồ thị phân tán (Graph Mining với GraphFrames)
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

#### 2.3. Môi trường Thực nghiệm chuẩn hóa (Dockerized Spark Lab)
* Đóng gói toàn bộ cụm thử nghiệm qua `Dockerfile` và `docker-compose.yml`:
  - Apache Spark 3.5.0, Python 3.11, PySpark, GraphFrames JAR (`graphframes:graphframes:0.8.3-spark3.5-s_2.12`), Hadoop AWS.
  - Tách biệt môi trường ảo nội bộ (`./venv/`), cấu hình chuẩn `SPARK_LOCAL_IP = "127.0.0.1"` và đồng bộ `PYSPARK_PYTHON = sys.executable`.

---

## 📂 III. BẢN ĐỒ CẤU TRÚC MÃ NGUỒN (REPOSITORY STRUCTURE)

```text
pyspark/ (DANGTHANH2k7/pyspark)
│
├── 📊 PHÂN HỆ CỦA ĐẶNG THÀNH (Core DataFrame & MLlib)
│   ├── basic.ipynb                           # Thao tác đọc file, schema, select, cast
│   ├── filter_operation.ipynb                # Các phép lọc có điều kiện phân tán
│   ├── groupby_aggregateFunc.ipynb           # Gom nhóm, aggregation & Map-side reduce
│   ├── null_value.ipynb                      # Xử lý khuyết thiếu với Imputer
│   ├── MLLib.ipynb                           # Thử nghiệm ban đầu với Spark MLlib
│   ├── LinearRegression/
│   │   ├── tips/                             # Bài toán hồi quy tiền boa (Tips dataset)
│   │   │   ├── tips.csv
│   │   │   ├── tips.ipynb
│   │   │   └── readme.md
│   │   └── house_prices/                     # Dự đoán giá nhà (Toán học L-BFGS & Log)
│   │       ├── house_prices.ipynb
│   │       ├── house_prices.csv.zip
│   │       └── README.md
│   └── test1.csv, test2.csv, test3.csv       # Dữ liệu kiểm thử
│
├── ⚙️ PHÂN HỆ CỦA ĐẶNG MINH QUÂN (Engine Internals, Profiler & GraphFrames)
│   ├── src/
│   │   ├── lessons/                          # Các bài học & thực hành chuẩn hóa
│   │   │   ├── 00_hello_spark.py             # Khởi tạo SparkSession chuẩn Apple Silicon
│   │   │   ├── wordCount.py                  # MapReduce kinh điển trên RDD
│   │   │   ├── day1_dataframe.py             # Transformation vs Action, Narrow vs Wide
│   │   │   └── self_day1_dataframe.py        # Code tự gõ thực hành và kiểm thử profiling
│   │   ├── graph/                            # Động cơ đồ thị phân tán
│   │   │   ├── economic_graph.py             # Xây dựng đồ thị VECOM, PageRank, LPA
│   │   │   └── self_lpa.py                   # Triển khai thực nghiệm thuật toán LPA
│   │   ├── etl/
│   │   │   └── 01_extract_vecom.py           # Pipeline ETL trích xuất thực thể đồ thị
│   │   └── utils/
│   │       └── profiler.py                   # Bộ đo Memory RSS Growth & Wall-clock Time
│   ├── LPA_Label_Propogation_Algorithm/       # Pipeline chuyên sâu thuật toán LPA
│   │   ├── communities.py                    # Khai phá cộng đồng
│   │   ├── graphx.py                         # Cấu trúc đồ thị phân tán
│   │   ├── read_data.py                      # Đọc và phân chia cạnh đồ thị
│   │   ├── results_new/                      # Kết quả benchmark xuất ra CSV
│   │   └── README.md                         # Báo cáo kỹ thuật thuật toán LPA
│   ├── Dockerfile                            # Image Spark 3.5 + GraphFrames + Jupyter
│   ├── docker-compose.yml                    # Khởi động cụm Spark phân tán cục bộ
│   ├── requirements.txt                      # Danh mục thư viện đồng bộ
│   ├── ROADMAP_1_WEEK_INTENSIVE_GRAPHFRAME.md# Lộ trình học 1 tuần cấp tốc
│   └── ROADMAP_4_WEEKS_GRAPHFRAME.md         # Lộ trình nghiên cứu toàn diện 4 tuần
│
└── 📋 TÀI LIỆU BÁO CÁO & ĐIỀU PHỐI
    └── BAO_CAO_TIEN_DO_SPARK.md               # Báo cáo tiến độ chung của nhóm
```

---

## 🎯 IV. KỊCH BẢN THUYẾT TRÌNH BÁO CÁO TIẾN ĐỘ (DÀNH CHO BUỔI BÁO CÁO NGÀY MAI)

### Thời lượng gợi ý: 8 - 10 phút
| Thời gian | Người trình bày | Nội dung trọng tâm |
| :--- | :--- | :--- |
| **0:00 - 1:30** | **Cả hai** | Giới thiệu mục tiêu chung: Làm chủ Spark từ lý thuyết tính toán phân tán đến ứng dụng thực tiễn (MLlib & Graph Mining). Giới thiệu phân công 2 nhánh. |
| **1:30 - 4:30** | **Đặng Thành** | Báo cáo nhánh DataFrame API & MLlib: Demo pipeline tiền xử lý `VectorAssembler`, mô hình hồi quy giá nhà, giải thuật tối ưu L-BFGS, và lý do toán học khi dùng Log-transform giúp giảm MAE $10.5\%$. |
| **4:30 - 7:30** | **Đặng Minh Quân** | Báo cáo nhánh Spark Engine & GraphFrames: Giải thích cơ chế Lazy Evaluation, demo đo lường hiệu năng với `profiler.py`, kiến trúc Docker Lab, thuật toán PageRank và phát hiện cộng đồng LPA với giải pháp `checkpoint()`. |
| **7:30 - 8:30** | **Cả hai** | Tổng kết thành quả đạt được, đối chiếu mục tiêu và nêu kế hoạch giai đoạn tiếp theo. |
| **8:30 - 10:00**| **Q&A** | Trả lời câu hỏi phản biện của Thầy / Mentor. |

---

## 💡 V. BỘ CÂU HỎI PHẢN BIỆN DỰ KIẾN (SOCRATIC Q&A)

### Câu hỏi 1: *"Tại sao Spark lại sử dụng cơ chế Lazy Evaluation thay vì thực thi ngay như Pandas?"*
* **Trả lời:**
  * Pandas thực thi ngay lập tức (Eager), sinh ra các DataFrame trung gian trong bộ nhớ sau mỗi dòng lệnh. Nếu làm việc với hàng trăm Gigabyte, RAM sẽ tràn ngay lập tức và không thể tối ưu.
  * Spark áp dụng **Lazy Evaluation**: Các lệnh Transformation (`filter`, `select`, `join`) chỉ ghi nhận vào đồ thị **DAG (Directed Acyclic Graph)**. Khi gặp một Action (`collect`, `write`), trình tối ưu **Catalyst Optimizer** mới phân tích toàn bộ DAG: tự động gộp các phép lọc (Predicate Pushdown), chiếu chỉ các cột cần thiết (Column Pruning) và nối các thao tác vào một vòng lặp trong RAM, giúp hạn chế tối đa chi phí ghi đĩa và truyền tải mạng.

### Câu hỏi 2: *"Phân biệt Narrow Dependency và Wide Dependency? Tại sao phải hạn chế Wide Dependency?"*
* **Trả lời:**
  * **Narrow Dependency (Phụ thuộc hẹp):** Mỗi partition của RDD cha chỉ được dùng bởi tối đa một partition của RDD con (ví dụ: `map`, `filter`). Dữ liệu xử lý hoàn toàn tại chỗ trong RAM của Executor hiện tại (Pipelineable), không cần trao đổi dữ liệu.
  * **Wide Dependency (Phụ thuộc rộng):** Nhiều partition của RDD con phụ thuộc vào dữ liệu từ các partition của RDD cha (ví dụ: `groupByKey`, `reduceByKey`, `join`). Hiện tượng này bắt buộc Spark phải kích hoạt **Shuffle**: ghi dữ liệu tạm ra đĩa, phân phối qua mạng mạng giữa các Executor. Shuffle là nút thắt cổ chai lớn nhất về I/O và độ trễ mạng trong hệ thống phân tán.

### Câu hỏi 3: *"Tại sao khi chạy các thuật toán lặp trên GraphFrames như PageRank hay LPA, bắt buộc phải gọi `checkpoint()`?"*
* **Trả lời:**
  * Thuật toán lặp thực hiện cập nhật trạng thái các đỉnh qua hàng chục hoặc hàng trăm vòng lặp. Mỗi vòng lặp lại tạo thêm các phép biến đổi mới nối dài đồ thị DAG RDD Lineage.
  * Nếu không cắt tỉa, cây phả hệ (Lineage Graph) quá dài sẽ khiến bộ phân tích của trình biên dịch JVM bị tràn ngăn xếp (**StackOverflowError**) và bộ nhớ RAM của Driver phình to. 
  * Lệnh `checkpoint()` sẽ ngắt đứt cây lineage dài dằng dặc, lưu trữ kết quả của vòng lặp hiện tại xuống bộ lưu trữ vật lý ổn định (Storage), và tái thiết lập một lineage mới tinh gọn từ điểm kiểm tra đó.

### Câu hỏi 4: *"Khi nào nên dùng Spark MLlib thay vì Scikit-Learn?"*
* **Trả lời:**
  * Scikit-Learn chỉ xử lý trên một máy đơn (Single-node), dữ liệu phải vừa khít với dung lượng RAM máy chủ. Khi kích thước dữ liệu vượt ngưỡng hàng chục triệu bản ghi hoặc hàng trăm Gigabyte, Scikit-Learn sẽ gặp lỗi `Out of Memory (OOM)`.
  * Spark MLlib được thiết kế chuyên biệt cho học máy phân tán trên cụm nhiều máy tính. Các giải thuật trong MLlib (như L-BFGS, ALS) được viết lại để tính toán gradient và ma trận song song trên từng phân vùng dữ liệu mà không cần gom toàn bộ dữ liệu về một máy.

---

## 🔮 VI. KẾ HOẠCH BƯỚC TIẾP THEO (NEXT STEPS)

1. **Tích hợp Pipeline End-to-End:** Kết hợp kết quả từ phân hệ Graph (Đặc trưng đồ thị như PageRank, LPA Community ID) làm biến đầu vào bổ sung cho mô hình Spark MLlib để cải thiện độ chính xác dự báo.
2. **Triển khai Delta Lake / Iceberg:** Nâng cấp tầng lưu trữ Silver/Gold từ Parquet thông thường lên bảng chuẩn ACID của Delta Lake để hỗ trợ Time Travel và Schema Enforcement.
3. **Mở rộng cụm phân tán:** Thử nghiệm triển khai pipeline trên môi trường đám mây Dataproc / EMR với khối lượng dữ liệu thực tế lớn (>100GB).
