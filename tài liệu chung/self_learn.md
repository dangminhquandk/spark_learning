# 📚 BÍ KÍP TỰ HỌC & LÀM CHỦ APACHE SPARK & GRAPHFRAMES

Tài liệu này tổng hợp toàn bộ kiến thức lý thuyết, nguyên lý hoạt động dưới tầng sâu (under the hood), và các kỹ thuật tối ưu hóa đã được thực hành trong dự án. Tài liệu được cấu trúc theo tiến trình tư duy từ Hệ thống lõi $\rightarrow$ Xử lý dữ liệu $\rightarrow$ Thuật toán Đồ thị $\rightarrow$ Tối ưu hóa bộ nhớ.

---

## ⚙️ 1. NỀN TẢNG KIẾN TRÚC SPARK (ENGINE INTERNALS)

### 1.1. Lazy Evaluation (Đánh giá lười biếng)
Spark không thực thi code ngay khi bạn viết. Các toán tử được chia làm 2 loại:
* **Transformations (Biến đổi):** `map`, `filter`, `groupBy`, `join`. Các lệnh này chỉ có tác dụng xây dựng **Đồ thị luồng thực thi (DAG - Directed Acyclic Graph)**.
* **Actions (Hành động):** `show`, `count`, `collect`, `write`. Chỉ khi gọi Action, Spark mới thực sự kích hoạt tính toán vật lý, duyệt ngược cây DAG từ cuối lên đầu để thực thi.
* **Tại sao?** Giúp Spark nhìn thấy toàn bộ bức tranh tính toán, từ đó gộp các phép toán lại với nhau (Pipelining) và tối ưu hóa trước khi chạy thực tế.

### 1.2. Kế hoạch thực thi Catalyst Optimizer
Khi gửi một truy vấn (SQL hoặc DataFrame API), Catalyst Optimizer sẽ phân tích qua 4 bước:
1. **Parsed Logical Plan:** Kiểm tra tính hợp lệ của cú pháp.
2. **Analyzed Logical Plan:** Đối chiếu với Catalog để kiểm tra cột/bảng có tồn tại không, kiểu dữ liệu là gì.
3. **Optimized Logical Plan:** Áp dụng các quy tắc tối ưu. Đáng chú ý nhất là **Predicate Pushdown** (đẩy các phép lọc `Filter` xuống sát nguồn đọc dữ liệu nhất để giảm dung lượng dữ liệu cần nạp vào RAM).
4. **Physical Plan:** Dịch kế hoạch logic thành kế hoạch vật lý thực thi trên Cụm (chọn thuật toán Join, HashAggregate, lên lịch Shuffle).

### 1.3. Shuffle Mechanics (Narrow vs Wide Dependency)
* **Narrow Dependency:** (VD: `filter`, `map`) Dữ liệu của một Partition con chỉ phụ thuộc vào một Partition cha. Phép toán xảy ra hoàn toàn in-memory trên cùng một node. Nhanh, cache-friendly.
* **Wide Dependency (Shuffle):** (VD: `groupBy`, `join`, `distinct`) Dữ liệu cần được xáo trộn và phân phối lại qua mạng tới các Node/Executor khác. Gây ra độ trễ I/O đĩa và mạng rất lớn.
* **Tối ưu:** Chủ động giảm cấu hình `spark.sql.shuffle.partitions` (mặc định 200) xuống con số nhỏ (ví dụ 4) khi chạy local hoặc tập dữ liệu nhỏ để tránh overhead sinh ra quá nhiều file nhỏ.

---

## 📊 2. KỸ THUẬT XỬ LÝ DỮ LIỆU (DATAFRAME API)

### 2.1. Explicit Schema vs InferSchema
* Tránh sử dụng `.option("inferSchema", "true")` vì nó ép Spark phải đọc toàn bộ dữ liệu 1 lần để đoán kiểu.
* **Thực hành tốt nhất:** Định nghĩa rõ ràng bằng `StructType` và `StructField`. Giúp Spark cấp phát bộ nhớ chuẩn xác qua Tungsten Engine, đọc nhanh hơn gấp nhiều lần.

### 2.2. Phép nối Left Anti Join
* Dùng để tìm các bản ghi có ở bảng Trái nhưng KHÔNG tồn tại ở bảng Phải.
* **Ứng dụng:** Phát hiện dữ liệu mồ côi, tài khoản bất hợp pháp, Fraud Detection (Ví dụ: Các giao dịch xuất phát từ số tài khoản không nằm trong hệ thống ngân hàng).

### 2.3. Window Functions
* Cho phép thực hiện các phép toán tổng hợp/xếp hạng trên một "cửa sổ" dữ liệu (Window) nhưng vẫn giữ nguyên từng dòng chi tiết (không bị gom gọn lại như `groupBy`).
* Ứng dụng: `dense_rank().over(Window.partitionBy("city").orderBy(desc("balance")))` để xếp hạng số dư trong từng thành phố mà không bị "thủng" thứ hạng.

---

## 🕸️ 3. KHAI PHÁ ĐỒ THỊ PHÂN TÁN (GRAPH MINING)

### 3.1. Mô hình hóa Đồ thị (Graph Modeling)
Quy đổi bài toán kinh tế/công nghệ về cấu trúc toán học trừu tượng $G = (V, E)$:
* **Đỉnh (Vertices - $V$):** Các thực thể (Tỉnh/Thành, URL bài viết). Yêu cầu bắt buộc phải có cột định danh `id`.
* **Cạnh (Edges - $E$):** Mối quan hệ định hướng (Dòng giao thương, Mentions). Yêu cầu bắt buộc phải có cột nguồn `src` và cột đích `dst`.
* **Tối ưu ID bằng Hàm băm (Hashing):** Băm các chuỗi URL dài bằng `SHA-1` và cắt 8 ký tự hexa. Kỹ thuật này giảm kích thước bộ nhớ JVM đột phá, tăng tốc độ phép Join đồ thị phân tán.

### 3.2. Thuật toán PageRank (Xác định Cực tăng trưởng)
* **Ý tưởng:** Một đỉnh được coi là quan trọng nếu nó được nhiều đỉnh quan trọng khác trỏ tới.
* **Toán học:** Phân phối xác suất dừng của một con trỏ đi lạc (random surfer) với Damping Factor $d = 0.85$ (hay Reset Probability $= 0.15$). 
* **Ứng dụng:** Tìm ra Hà Nội, TP.HCM là các cực hút dòng tiền (Economic Hubs).

### 3.3. Thuật toán Lan truyền nhãn (Label Propagation Algorithm - LPA)
* **Ý tưởng:** Phát hiện các cụm (Community Detection) mà không cần khai báo trước số lượng (như K-Means).
* **Toán học:** Ban đầu mỗi đỉnh có nhãn riêng. Qua mỗi vòng lặp $t$, đỉnh cập nhật nhãn của mình bằng nhãn **chiếm đa số** trong tập các đỉnh láng giềng. Các đỉnh liên kết dày đặc sẽ dần đồng thuận về cùng 1 nhãn.
* **Ứng dụng:** Tìm ra các liên kết vùng kinh tế tự nhiên, hoặc các cụm công nghệ có liên hệ mật thiết (Machine Learning, Big Data, Cloud Computing).

---

## ☠️ 4. VẤN ĐỀ SỐNG CÒN TRONG HỆ PHÂN TÁN (CRUCIAL GOTCHAS)

### 4.1. Tràn ngăn xếp JVM (StackOverflowError) do RDD Lineage Bloat
* **Vấn đề:** Các thuật toán đồ thị (LPA, PageRank) là các thuật toán **lặp (Iterative)**. Trạng thái vòng lặp sau kế thừa vòng lặp trước. Spark sẽ ghi nhớ toàn bộ chuỗi phụ thuộc này vào cây phả hệ (RDD Lineage). Chỉ sau khoảng 10 vòng lặp, cây Lineage sẽ quá dài. Khi gọi Action, JVM duyệt đệ quy cây này và gây ra lỗi `StackOverflowError`.
* **Khắc phục (Checkpointing):** Sử dụng `spark.sparkContext.setCheckpointDir("/path/")`. Chức năng này cắt đứt (truncate) chuỗi Lineage, ghi trạng thái tính toán trung gian xuống ổ cứng an toàn, giúp giải phóng bộ nhớ và giữ cây phả hệ luôn ngắn gọn.

### 4.2. Quản lý Cache & Số lượng file phân tán
* **`.persist()`:** Với dữ liệu dùng lại nhiều lần (ví dụ như đối tượng `communities`), gọi `.persist()` để cache vào RAM, tránh việc Spark phải tính toán lại từ file thô ở các câu lệnh sau.
* **`.coalesce(1)`:** Khi xuất file CSV ở cụm phân tán, dữ liệu bị xé lẻ thành vô số file `part-*.csv`. Gọi `.coalesce(1)` giúp thu gom dữ liệu về chung 1 phân vùng để xuất ra 1 file duy nhất.

---

## 🐳 5. MÔI TRƯỜNG & TRIỂN KHAI (DOCKER)
Hệ sinh thái Big Data (Spark, Java, Scala, Python, Hadoop) rất nhạy cảm với xung đột phiên bản.
* Đóng gói toàn bộ vào `Dockerfile` và `docker-compose.yml` đảm bảo tính tái lập (Reproducibility).
* Ai cũng có thể chạy dự án thông qua duy nhất 1 lệnh `docker compose up -d` mà không cần setup rườm rà. Lấy kết quả nhất quán trên mọi hệ điều hành.

---
*Tài liệu này là thành quả của quá trình liên tục mô hình hóa bài toán, xác lập tiệm cận hiệu năng và bóc tách từng bottleneck của hệ thống phân tán.*
