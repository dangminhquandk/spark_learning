## BÁO CÁO KỸ THUẬT: PHÂN TÍCH VÀ GIẢI THÍCH CHI TIẾT CÁC HÀM TRONG PIPELINE HỒI QUY TUYẾN TÍNH PHÂN TÁN (PYSPARK MLLIB)

---

### 1. Khởi tạo môi trường và Quản trị phiên thực thi (Session Management)

* **`SparkSession.builder.appName(...).getOrCreate()`**:


* *Bản chất kỹ thuật:* `SparkSession` là điểm điều khiển thống nhất (unified entry point) của Spark từ phiên bản 2.0 trở đi, thay thế và tích hợp các ngữ cảnh rời rạc trước đây (`SparkContext`, `SQLContext`, `HiveContext`). Pattern `.builder` thiết lập cấu hình và kết nối tiến trình Python với máy ảo Java (JVM) thông qua cầu nối Py4J socket. Phương thức `.getOrCreate()` kiểm tra nếu đã tồn tại một session khả dụng thì tái sử dụng, ngược lại sẽ khởi tạo phiên mới trên Driver.


* *Mô hình phân tán:* Khởi tạo tiến trình Driver – nơi nắm giữ đồ thị phụ thuộc (DAG), kích hoạt bộ tối ưu hóa truy vấn Catalyst và lập lịch phân bổ task xuống các Executor.




* **`spark.stop()`**:


* *Bản chất kỹ thuật:* Đóng toàn bộ ngữ cảnh thực thi của ứng dụng Spark. Hàm này giải phóng tài nguyên bộ nhớ JVM của Executor, dừng các luồng worker, đóng kết nối Py4J gateway và giải phóng cổng mạng Spark UI (mặc định là cổng `4040`) để tránh xung đột cổng khi chạy các phiên tính toán tiếp theo.





---

### 2. Nạp dữ liệu, Biến đổi bảng và Kích hoạt Action (Structured APIs)

Theo nguyên lý của Spark, các hàm được chia làm hai loại: **Transformations** (biến đổi dữ liệu có tính chất lazy) và **Actions** (kích hoạt tính toán thực tế trên cụm).

* **`spark.read.csv(path, header=True, inferSchema=True)`**:


* *Bản chất kỹ thuật:* Là một Dataframe Reader API. Cờ `header=True` lấy dòng đầu làm tên cột; `inferSchema=True` yêu cầu Catalyst quét qua một phân vùng dữ liệu để tự động nhận dạng kiểu dữ liệu nguyên thủy (phân biệt số thực dấu phẩy động 64-bit `DoubleType`, số nguyên `IntegerType` và chuỗi `StringType`) thay vì mặc định chuỗi toàn bộ.


* *Cơ chế Lazy Evaluation:* Lệnh này chưa nạp toàn bộ file vào RAM mà chỉ định hình metadata và schema trong đồ thị thực thi của Catalyst.




* **`df.show(n, truncate=False)`**:


* *Bản chất kỹ thuật:* Là một **Action**. Nó gửi yêu cầu tính toán xuống DAG Scheduler để thực thi các biến đổi trước đó, thu thập $n$ dòng đầu tiên từ các partition của Executor gửi về Driver và định dạng hiển thị thành bảng console. Tham số `truncate=False` giữ nguyên độ dài chuỗi hiển thị, giúp quan sát trọn vẹn vector nhiều chiều.




* **`df.printSchema()`**:


* *Bản chất kỹ thuật:* Không phải là Action tính toán dữ liệu. Hàm này chỉ in ra cấu trúc cây metadata kiểu dữ liệu hiện thời do bộ phân tích Catalyst quản lý.




* **`df.columns`**:


* *Bản chất kỹ thuật:* Thuộc tính trả về danh sách tên cột dạng mảng Python `list` trực tiếp từ schema của DataFrame trên Driver.




* **`df.select(*cols)`**:


* *Bản chất kỹ thuật:* Là một **Transformation**. Thực hiện kỹ thuật tối ưu hóa cắt tỉa cột (Column Pruning). Nó tạo ra DataFrame mới chỉ giữ lại các trường thông tin cần thiết (`Independent Features` và nhãn `tip`), chỉ dẫn Catalyst loại bỏ các cột không sử dụng khỏi kế hoạch vật lý để tiết kiệm bộ nhớ RAM và giảm tải chi phí tuần tự hóa (serialization) qua mạng.




* **`df.count()`**:


* *Bản chất kỹ thuật:* Là một **Action**. Kích hoạt các Executor đếm song song số lượng dòng dữ liệu trên từng phân vùng cục bộ, sau đó Driver tổng hợp các kết quả thành tổng số quan sát $N$.





---

### 3. Kỹ nghệ đặc trưng phân tán (Feature Engineering)

Trong Spark MLlib, các giải thuật biến đổi dữ liệu tuân theo hai chuẩn thiết kế: **Estimator** (phải học tham số qua `.fit()`) và **Transformer** (áp dụng biến đổi qua `.transform()`).

```
Raw Columns (String) 
       ↓ StringIndexer.fit() & transform()
Indexed Columns (0.0, 1.0, ...)
       ↓ VectorAssembler.transform()
Unified Feature Vector (Dense/Sparse)

```

#### 3.1. `StringIndexer(inputCols=..., outputCols=...)`

* **Bản chất kỹ thuật:** Là một **Estimator**. Nó duyệt song song trên các cột chuỗi hạng mục (`sex`, `smoker`, `day`, `time`) để thu thập các phần tử duy nhất và tần suất của chúng.


* **Phương thức `.fit(df)`:** Quét toàn bộ tập dữ liệu để học và sinh ra đối tượng `StringIndexerModel` (đóng vai trò là một Transformer) chứa bảng từ điển ánh xạ cố định.


* **Phương thức `.transform(df)`:** Áp bảng từ điển để tạo ra các cột số mới (`DoubleType`).


* **Cơ sở toán học:** Ánh xạ từ không gian hạng mục định danh $\mathcal{C} = \{c_1, c_2, \dots, c_K\}$ sang tập số nguyên thực $\{0.0, 1.0, \dots, K-1\}$ theo thứ tự giảm dần của tần suất xuất hiện:



$$\text{Index}(c) = \operatorname{rank}_{\text{desc}}\Big(\vert{}\{i : x^{(i)} = c\}\vert{}\Big) \quad \text{với } \text{Index}(c_{(0)}) = 0.0 \text{ cho nhãn phổ biến nhất}$$




#### 3.2. `VectorAssembler(inputCols=..., outputCol=...)`

* **Bản chất kỹ thuật:** Là một **Transformer** thuần túy (chỉ cần gọi `.transform()` mà không cần `.fit()`). Nó gom các cột số vô hướng và cột chỉ số phân loại thành một cột vector đặc trưng duy nhất.


* **Tối ưu hóa bộ nhớ:** Tự động quyết định lưu trữ dưới dạng:
* *DenseVector:* Mảng các số thực liên tục $\mathbf{v}_{\text{dense}} = [v_0, v_1, \dots, v_{d-1}]$.


* *SparseVector:* Bộ ba gồm kích thước, mảng chỉ số khác không và mảng giá trị khác không $\mathbf{v}_{\text{sparse}} = (d, [i_0, \dots, i_{k-1}], [v_{i_0}, \dots, v_{i_{k-1}}])$ để tiết kiệm tối đa dung lượng bộ nhớ khi có nhiều giá trị mang số $0.0$.




* **Cơ sở toán học:** Gom $d = 6$ thuộc tính rời rạc thành vector thiết kế không gian đa chiều $\mathbf{x}^{(i)} \in \mathbb{R}^6$ cho quan sát thứ $i$:



$$\mathbf{x}^{(i)} = [x_{\text{total\_bill}}^{(i)}, x_{\text{size}}^{(i)}, x_{\text{sex}}^{(i)}, x_{\text{smoker}}^{(i)}, x_{\text{day}}^{(i)}, x_{\text{time}}^{(i)}]^T$$




Định dạng này tương thích trực tiếp với các thư viện tính toán đại số tuyến tính cấp thấp (BLAS/LAPACK) của JVM mà không cần tái cấu trúc dữ liệu.



---

### 4. Phân vùng dữ liệu và Huấn luyện mô hình (Model Training & Optimization)

* **`df.randomSplit([0.75, 0.25], seed=42)`**:


* *Bản chất kỹ thuật:* Là một **Transformation** dạng Narrow (không yêu cầu Shuffle mạng giữa các nút). Mỗi Executor tự quyết định dòng dữ liệu trên phân vùng của mình thuộc tập Train hay Test thông qua phép thử Bernoulli độc lập.


* *Vai trò của `seed=42`:* Đảm bảo tính tất định (determinism). Nhờ hạt giống cố định, nếu một node bị lỗi thì Spark có thể tái lập đúng dữ liệu phân vùng đó từ Lineage RDD mà không gây lệch mẫu. Đồng thời, nó ngăn chặn hiện tượng rò rỉ dữ liệu (data leakage) giữa các lần chạy thử nghiệm.




* **`LinearRegression(featuresCol='Independent Features', labelCol='tip')`**:


* *Bản chất kỹ thuật:* Là một **Estimator** đại diện cho bài toán Hồi quy tuyến tính đa biến. Nhãn mục tiêu được cấu hình chuẩn xác là tiền boa `tip` ($y \in \mathbb{R}$), và vector đặc trưng đầu vào là `Independent Features` ($\mathbf{x} \in \mathbb{R}^6$).




* **`regressor = lr.fit(train_data)`**:


* *Bản chất kỹ thuật:* Kích hoạt việc huấn luyện trên các Executor để giải bài toán tối ưu, sinh ra đối tượng mô hình `LinearRegressionModel` (một Transformer).


* *Cơ sở toán học (Giả thuyết tuyến tính & OLS):*
Mô hình xác lập mối quan hệ dự báo:



$$\hat{y}^{(i)} = \mathbf{w}^T \mathbf{x}^{(i)} + b = b + \sum_{j=1}^{d} w_j x_j^{(i)}$$




Nghiệm tối ưu đạt được bằng cách cực tiểu hóa hàm mất mát tổng bình phương sai số (Ordinary Least Squares - OLS) trên $N$ quan sát:



$$\min_{\mathbf{w}, b} J(\mathbf{w}, b) = \frac{1}{2N} \sum_{i=1}^{N} \left( y^{(i)} - (\mathbf{w}^T \mathbf{x}^{(i)} + b) \right)^2 = \frac{1}{2N} \Vert{}\mathbf{y} - (\mathbf{X}\mathbf{w} + b\mathbf{1})\Vert{}_2^2$$




Giải hệ phương trình đạo hàm riêng $\nabla_{\mathbf{w}} J = \mathbf{0}$ quy về giải phương trình chuẩn tắc (Normal Equation) phân tán hoặc thuật toán quasi-Newton (L-BFGS):



$$\mathbf{w}^* = (\mathbf{X}^T \mathbf{X})^{-1} \mathbf{X}^T \mathbf{y}$$





* **`regressor.intercept` & `regressor.coefficients` **:


* *Bản chất toán học:*
* `intercept` ($b \approx 0.6422$): Tiền boa cơ sở kỳ vọng khi toàn bộ các biến độc lập nhận giá trị 0.


* `coefficients` ($\mathbf{w} \in \mathbb{R}^6$): Đạo hàm riêng $w_j = \frac{\partial \hat{y}}{\partial x_j}$, biểu thị sự thay đổi biên của tiền boa khi thuộc tính $x_j$ tăng thêm một đơn vị trong điều kiện các yếu tố khác không đổi. Cụ thể, hệ số $w_{\text{total\_bill}} \approx +0.1048$ chỉ ra rằng với mỗi USD hóa đơn tăng thêm, tiền boa ước tính tăng trung bình khoảng 0.105 USD.







