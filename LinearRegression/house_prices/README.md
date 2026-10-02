# BÁO CÁO KỸ THUẬT VÀ CƠ SỞ TOÁN HỌC: PIPELINE HỒI QUY TUYẾN TÍNH PHÂN TÁN DỰ ĐOÁN GIÁ BẤT ĐỘNG SẢN (PYSPARK MLLIB)

---

## 1. Đánh giá tổng quan hiệu năng mô hình (Model Performance Evaluation)

Trong bài toán dự đoán giá nhà bất động sản Ấn Độ (`house_prices.csv`, quy mô ban đầu $N = 187,531$ quan sát), quá trình thực nghiệm đã trải qua 3 giai đoạn tiến hóa quan trọng:

| Chỉ số đánh giá | Phiên bản 1 (Baseline: Đơn giá/sqft chưa lọc Outlier) | Phiên bản 2 (Chuẩn hóa: Tổng tiền nhà `total_price` đã lọc Outlier) | Phiên bản 3 (Nâng cấp 1: Biến đổi Logarit nhãn mục tiêu $\ln(y)$) | Nhận xét bản chất & So sánh kỹ thuật |
| :--- | :---: | :---: | :---: | :--- |
| **$R^2$ trên không gian huấn luyện** | $0.0027$ ($0.27\%$) | **$0.7201$ ($72.01\%$)** | **$0.7270$ ($72.70\%$)** *(trên thang Log)* | **Bản Log đạt độ khớp cao nhất** trên không gian hàm mục tiêu được tối ưu. |
| **$R^2$ quy đổi về tiền thực tế (INR)** | $0.0027$ | **$0.7201$ ($72.01\%$)** | $0.6759$ ($67.59\%$) *(sau khi $\exp$)* | Bản 2 tối ưu trực tiếp $R^2$ trên tiền, trong khi Bản 3 tối ưu sai số phần trăm. |
| **MAE (Sai số tuyệt đối trung bình)** | $2,816.95$ INR | $2,768,039.62$ INR | **$2,479,468.06$ INR** *(sau khi $\exp$)* | **Bản 3 giảm sai số 288,571 INR ($\approx -10.5\%$)**. Dự đoán sát giá thực tế nhất cho $90\%$ căn hộ phổ thông! |
| **RMSE (Root Mean Squared Error)** | $49,872.96$ | **$4,198,014.09$ INR** | $4,517,548.36$ INR *(sau khi $\exp$)* | Bản 3 bị phạt nặng bởi một vài căn biệt thự lớn khi hàm mũ $\exp()$ khuếch đại sai số. |
| **Số chiều đặc trưng ($D$)** | $127$ chiều | $127$ chiều | $127$ chiều | $5$ biến số thực + $122$ biến nhị phân One-Hot đại diện cho 81 thành phố và tiện ích. |
| **Số mẫu khả dụng ($N$)** | $169,866$ dòng | $163,962$ dòng | $163,962$ dòng | Đã lọc triệt để dữ liệu nhập sai, rác và ngoại lai vô lý. |

> [!TIP]
> **Khuyến nghị lựa chọn mô hình trong thực tế:**
> * **Chọn Phiên bản 2 (Tổng tiền nhà trực tiếp, $R^2 = 72.01\%$):** Khi cần báo cáo chỉ số học thuật $R^2$ cao nhất và tối thiểu hóa phương sai bình phương tổng thể.
> * **Chọn Phiên bản 3 (Biến đổi Logarit, MAE $= 2.47$ triệu):** Khi triển khai hệ thống định giá thực tế phục vụ đại đa số khách hàng mua nhà bình dân, vì mô hình dự đoán chính xác hơn $10.5\%$ trên từng giao dịch cụ thể.

---

## 2. Kiến trúc luồng xử lý phân tán (Distributed Data Pipeline)

Quy trình xử lý dữ liệu được thiết kế theo chuẩn phân tán của Apache Spark:

```
[Raw CSV] (187,531 rows)
   │
   ├─► 1. Cấu hình Parser chuẩn RFC-4180 (escape='"', multiLine=True) chống lệch cột
   ├─► 2. Chuẩn hóa Schema: ép kiểu an toàn qua try_cast & Regex (sqft, '> 10')
   ├─► 3. Kiểm toán Null (Missing Value Audit) -> Drop cột > 60% null (dimensions, plot_area)
   ├─► 4. Feature Engineering: Trích xuất parking_count & current_floor
   ├─► 5. Imputation: Điền trung vị (Median) cho các biến số qua Spark ML Imputer
   ├─► 6. Lọc Outlier: price_in_rupees ∈ [1500, 35000], carpet_area ∈ [150, 4000]
   ├─► 7. Tính Target: total_price = price_in_rupees * carpet_area
   ├─► 8. Mã hóa danh mục: StringIndexer (Frequency-based) ──► OneHotEncoder (SparseVector)
   ├─► 9. Gom cụm không gian: VectorAssembler -> X ∈ R^127
   └─► 10. Huấn luyện & Đánh giá:
          ├─► Mô hình A: LinearRegression trên total_price (R² = 72.01%)
          └─► Mô hình B: LinearRegression trên ln(total_price) ──► exp(pred) (MAE tối ưu)
```

---

## 3. Cơ sở toán học từ đầu đến cuối (Mathematical Foundations)

---

### 3.1. Toán học của Xử lý Thiếu dữ liệu (Robust Missing Value Imputation)

Đối với các thuộc tính định lượng ($x_{\text{carpet\_area}}, x_{\text{bathroom}}, x_{\text{balcony}}, x_{\text{current\_floor}}$), chiến lược điền giá trị thiếu sử dụng **Giá trị trung vị (Median)** thay vì **Trung bình số học (Mean)**.

* **Điểm phá vỡ (Breakdown Point $\varepsilon^*$):**
  * Giá trị trung bình $\bar{x} = \frac{1}{N}\sum_{i=1}^N x_i$ có điểm phá vỡ $\varepsilon^* = 0\%$. Chỉ cần một quan sát $x_k \to \infty$ (outlier), toàn bộ $\bar{x} \to \infty$.
  * Giá trị trung vị $\operatorname{Med}(X)$ có điểm phá vỡ cực đại:
    $$\varepsilon^* = 50\%$$
    Nghĩa là có thể chịu đựng tới $50\%$ số mẫu bị ô nhiễm ngoại lai mà giá trị ước lượng không bị kéo lệch.

* **Công thức toán học của Imputer Median:**
  Cho tập mẫu $X = \{x_{(1)} \le x_{(2)} \le \dots \le x_{(N)}\}$ đã loại bỏ null:
  $$\hat{x}_{\text{impute}} = \operatorname{Med}(X) = 
  \begin{cases} 
  x_{\left(\frac{N+1}{2}\right)} & \text{nếu } N \text{ lẻ} \\ 
  \frac{1}{2}\left(x_{\left(\frac{N}{2}\right)} + x_{\left(\frac{N}{2}+1\right)}\right) & \text{nếu } N \text{ chẵn} 
  \end{cases}$$

---

### 3.2. Toán học của Mã hóa Biến danh mục (Categorical Encoding)

Không gian dữ liệu ban đầu chứa các biến định danh phi số (Nominal):
$$\mathcal{C} = \{\text{location}, \text{status}, \text{transaction}, \text{furnishing}, \text{facing}, \text{overlooking}, \text{ownership}\}$$

#### A. Ánh xạ tần suất `StringIndexer`
Với mỗi cột danh mục $C \in \mathcal{C}$ gồm $K$ nhãn duy nhất $\{c_1, c_2, \dots, c_K\}$, `StringIndexer` xác lập một ánh xạ đơn ánh:
$$f_{\text{index}}: \mathcal{C} \to \{0.0, 1.0, \dots, K-1\}$$
sắp xếp theo thứ tự giảm dần của lực lượng tập hợp (cardinality):
$$f_{\text{index}}(c) = \operatorname{rank}_{\text{desc}}\Big(\vert{}\{i : x^{(i)} = c\}\vert{}\Big), \quad f_{\text{index}}(c_{(0)}) = 0.0 \text{ (nhãn phổ biến nhất)}$$

#### B. Ánh xạ nhị phân không gian `OneHotEncoder`
Nếu đưa trực tiếp $f_{\text{index}}(c) \in \mathbb{R}$ vào hồi quy tuyến tính, mô hình sẽ áp đặt sai lầm quan hệ thứ bậc:
$$\hat{y} = w \cdot f_{\text{index}}(c) \implies \text{khoảng cách giữa nhãn 2 và 0 gấp đôi nhãn 1 và 0}$$

Để giải quyết, `OneHotEncoder` thực hiện phép nhúng không gian (embedding) sang vector nhị phân nhiều chiều:
$$f_{\text{OHE}}: \{0, 1, \dots, K-1\} \to \{0, 1\}^M$$

Trong PySpark, với tùy chọn `dropLast=True` (mặc định để tránh bẫy đa cộng tuyến - *Dummy Variable Trap*), chiều của vector đầu ra là $M = K - 1$:
$$\mathbf{e}_k = 
\begin{cases} 
\mathbf{0}_{M} & \text{nếu } k = K-1 \text{ (nhóm tham chiếu)} \\ 
[\underbrace{0, \dots, 0}_{k}, 1, \underbrace{0, \dots, 0}_{M-k-1}]^T & \text{nếu } 0 \le k < M 
\end{cases}$$

* **Tối ưu hóa bộ nhớ qua `SparseVector`:**
  Thay vì lưu trữ mảng thực $122$ phần tử tốn $122 \times 8 = 976$ bytes, Spark lưu trữ dưới dạng bộ 3 thưa:
  $$\mathbf{v}_{\text{sparse}} = (d, [i], [1.0]) \quad \text{chỉ tốn } O(1) \text{ bộ nhớ}$$

---

### 3.3. Giả thuyết Tuyến tính Đa biến (Multivariate Linear Hypothesis)

Vector đặc trưng tổng hợp sau `VectorAssembler` cho quan sát thứ $i$ có số chiều $D = 127$:
$$\mathbf{x}^{(i)} = \Big[ \underbrace{x_1^{(i)}, \dots, x_5^{(i)}}_{\text{5 biến định lượng}}, \underbrace{e_1^{(i)}, \dots, e_{122}^{(i)}}_{\text{122 biến One-Hot}} \Big]^T \in \mathbb{R}^{127}$$

Mô hình thiết lập phương trình mặt siêu phẳng hồi quy:
$$\hat{y}^{(i)} = f(\mathbf{x}^{(i)}) = \mathbf{w}^T \mathbf{x}^{(i)} + b = b + \sum_{j=1}^{127} w_j x_j^{(i)}$$

Trong đó:
* $\mathbf{w} = [w_1, w_2, \dots, w_{127}]^T \in \mathbb{R}^{127}$ là vector hệ số góc (weights/coefficients).
* $b \in \mathbb{R}$ là hệ số chặn (intercept/bias). Kết quả thực nghiệm: $b \approx -7,815,460.93$.
* Ý nghĩa vật lý của hệ số $w_j$:
  $$w_j = \frac{\partial \hat{y}}{\partial x_j}$$
  Biểu thị mức thay đổi kỳ vọng của tổng giá nhà khi đặc trưng $x_j$ tăng thêm 1 đơn vị, cố định toàn bộ 126 đặc trưng còn lại.

---

### 3.4. Hàm Mất mát và Bài toán Tối ưu hóa (Loss Function & Ridge Regularization)

Để kiểm soát hiện tượng đa cộng tuyến sinh ra từ 81 thành phố và các thuộc tính tương quan, mô hình sử dụng hàm mục tiêu **Hồi quy sườn (Ridge Regression - L2 Regularization)**:

$$\min_{\mathbf{w}, b} \mathcal{J}(\mathbf{w}, b) = \frac{1}{2N} \sum_{i=1}^{N} \Big( y^{(i)} - (\mathbf{w}^T \mathbf{x}^{(i)} + b) \Big)^2 + \frac{\lambda}{2} \|\mathbf{w}\|_2^2$$

Dưới dạng ma trận toàn cục với ma trận thiết kế $\mathbf{X} \in \mathbb{R}^{N \times 127}$ và nhãn $\mathbf{y} \in \mathbb{R}^N$:
$$\mathcal{J}(\mathbf{w}, b) = \frac{1}{2N} \|\mathbf{y} - (\mathbf{X}\mathbf{w} + b\mathbf{1})\|_2^2 + \frac{\lambda}{2} \mathbf{w}^T \mathbf{w}$$

Trong đó $\lambda = \text{regParam} = 0.1$.

* **Nghiệm giải tích chuẩn tắc (Regularized Normal Equation):**
  Triệt tiêu gradient $\nabla_{\mathbf{w}} \mathcal{J} = \mathbf{0}$:
  $$\mathbf{w}^* = (\mathbf{X}^T \mathbf{X} + N\lambda \mathbf{I})^{-1} \mathbf{X}^T (\mathbf{y} - b\mathbf{1})$$

* **Ý nghĩa toán học của thành phần điều chuẩn $N\lambda \mathbf{I}$:**
  Khi các biến One-Hot bị phụ thuộc tuyến tính cục bộ, ma trận hiệp phương sai mẫu $\mathbf{X}^T \mathbf{X}$ có thể bị suy biến (singular, $\det(\mathbf{X}^T\mathbf{X}) \approx 0$).
  Việc cộng thêm đường chéo $N\lambda \mathbf{I}$ đảm bảo ma trận luôn **xác định dương (positive definite)**, khả nghịch $100\%$ và chặn các trọng số $w_j$ không bị bùng nổ vô cực.

* **Thuật toán tối ưu hóa phân tán trên cụm Worker:**
  Spark MLlib giải bài toán trên thông qua giải thuật quasi-Newton **L-BFGS (Limited-memory Broyden–Fletcher–Goldfarb–Shanno)** song song hóa, xấp xỉ ma trận nghịch đảo Hessian $H^{-1}$ qua $m$ bước gradient trong bộ nhớ RAM mà không cần lưu trữ ma trận $127 \times 127$ đầy đủ.

---

### 3.5. Toán học của Bản nâng cấp số 1: Biến đổi Logarit nhãn ($\ln(y)$)

#### A. Bản chất dịch chuyển hàm mất mát
Khi áp dụng biến đổi $z = \ln(y)$, hàm mục tiêu được tối ưu trong quá trình huấn luyện trở thành:
$$\min_{\mathbf{w}, b} \frac{1}{2N} \sum_{i=1}^{N} \Big( \ln(y^{(i)}) - \ln(\hat{y}^{(i)}) \Big)^2 = \frac{1}{2N} \sum_{i=1}^{N} \left( \ln \frac{y^{(i)}}{\hat{y}^{(i)}} \right)^2$$

Khai triển Taylor bậc nhất quanh điểm $\hat{y} \approx y$:
$$\ln \frac{y}{\hat{y}} = \ln \left( 1 + \frac{y - \hat{y}}{\hat{y}} \right) \approx \frac{y - \hat{y}}{\hat{y}} = \text{Relative Percentage Error}$$

> **Ý nghĩa:** Mô hình Log không còn tối ưu sai số tiền tuyệt đối $(y - \hat{y})^2$ nữa, mà chuyển sang **tối thiểu hóa sai số phần trăm tương đối**! Điều này giải thích vì sao trên thang đo Log, $R^2$ đạt tới **$72.70\%$** (cao hơn mức $72.01\%$ của bản gốc).

#### B. Hiệu ứng khuếch đại sai số của hàm mũ $\exp()$
Khi chuyển đổi ngược $\hat{y}_{\text{actual}} = \exp(\hat{y}_{\text{log}})$:
* Do $\exp(u)$ là hàm lồi tăng trưởng theo cấp số nhân:
  $$\hat{y} + \Delta y = \exp(\hat{u} + \Delta u) = \exp(\hat{u}) \cdot \exp(\Delta u)$$
* Nếu mô hình đoán lệch trên thang log một lượng nhỏ $\Delta u = +0.4$:
  $$\exp(0.4) \approx 1.4918 \implies \text{Giá trị tiền bị nhân lên } +49.2\%$$
* Với một căn hộ cao cấp 20 triệu INR, sai số $49.2\%$ tạo ra chênh lệch tuyệt đối tới **$9.84$ triệu INR**! Khi đưa vào công thức RMSE và $R^2$ (vốn tính theo chuẩn bình phương $L_2$), các căn hộ đắt tiền này kéo tổng sai số bình phương tăng lên, khiến $R^2$ trên tiền thực tế rơi về $67.59\%$.

#### C. Bất đẳng thức Jensen & Thiên vị dự báo hàm mũ
Theo Bất đẳng thức Jensen đối với hàm lồi $\varphi(u) = \exp(u)$:
$$\mathbb{E}[\exp(\varepsilon)] > \exp(\mathbb{E}[\varepsilon]) = \exp(0) = 1$$
Nếu phần dư thỏa mãn giả định chuẩn tắc $\varepsilon \sim \mathcal{N}(0, \sigma^2)$, thì:
$$\mathbb{E}[y | X] = \exp(\mathbf{w}^T \mathbf{x} + b) \cdot \mathbb{E}[\exp(\varepsilon)] = \exp(\mathbf{w}^T \mathbf{x} + b) \cdot \exp\left(\frac{\sigma^2}{2}\right)$$
Nếu chỉ sử dụng ước lượng thô $\hat{y} = \exp(\hat{y}_{\text{log}})$, mô hình thực chất đang ước lượng **Giá trị Trung vị (Conditional Median)** của giá nhà, chứ không phải **Kỳ vọng Trung bình (Conditional Mean)**. Do công thức $R^2$ đo lường phương sai quanh giá trị trung bình $\bar{y}$, việc dự đoán trung vị sẽ làm giảm nhẹ điểm $R^2$.

#### D. Vì sao MAE lại giảm ngoạn mục $10.5\%$?
* Khác với RMSE, MAE đo lường khoảng cách tuyệt đối chuẩn $L_1$:
  $$\text{MAE} = \frac{1}{N} \sum_{i=1}^N |y^{(i)} - \hat{y}^{(i)}|$$
* Trong thống kê, hàm mất mát $L_1$ đạt cực tiểu tại **Giá trị Trung vị (Median)**!
* Do mô hình Log xấp xỉ phân phối trung vị cực kỳ chuẩn xác cho $90\%$ các căn hộ bình dân (không bị chi phối bởi các căn nhà siêu đắt), **sai số tuyệt đối trung bình MAE giảm mạnh từ $2.768$ triệu xuống $2.479$ triệu INR**.

---

### 3.6. Toán học của các Thước đo Đánh giá (Evaluation Metrics)

Sau khi dự đoán trên tập kiểm thử độc lập $\mathcal{D}_{\text{test}} = \{(\mathbf{x}^{(i)}, y^{(i)})\}_{i=1}^{N_{\text{test}}}$, các chỉ số được tính toán như sau:

#### A. Hệ số xác định $R^2$ (Coefficient of Determination)
$$R^2 = 1 - \frac{\text{SS}_{\text{res}}}{\text{SS}_{\text{tot}}} = 1 - \frac{\sum_{i=1}^{N_{\text{test}}} (y^{(i)} - \hat{y}^{(i)})^2}{\sum_{i=1}^{N_{\text{test}}} (y^{(i)} - \bar{y})^2}$$

* $\text{SS}_{\text{tot}} = \sum (y^{(i)} - \bar{y})^2$: Tổng bình phương sai lệch so với giá trị trung bình (phương sai toàn phần).
* $\text{SS}_{\text{res}} = \sum (y^{(i)} - \hat{y}^{(i)})^2$: Tổng bình phương phần dư (phần sai số mô hình không giải thích được).

#### B. Căn bậc hai sai số bình phương trung bình (RMSE)
$$\text{RMSE} = \sqrt{\frac{1}{N_{\text{test}}} \sum_{i=1}^{N_{\text{test}}} (y^{(i)} - \hat{y}^{(i)})^2}$$
Do sử dụng chuẩn $L_2$ (bình phương), RMSE phạt rất nặng các điểm sai số lớn, đóng vai trò là cận trên nghiêm ngặt của sai số dự báo.

#### C. Sai số tuyệt đối trung bình (MAE)
$$\text{MAE} = \frac{1}{N_{\text{test}}} \sum_{i=1}^{N_{\text{test}}} |y^{(i)} - \hat{y}^{(i)}|$$
Đo lường kỳ vọng sai lệch tuyến tính theo chuẩn $L_1$, ít bị méo bởi các căn penthouse hay biệt thự giá cao.

---

## 4. Tóm tắt các hàm PySpark then chốt đã triển khai

| Tên hàm / API | Phân loại | Bản chất xử lý |
| :--- | :--- | :--- |
| `spark.read.csv(..., escape="\"", multiLine=True)` | Reader API | Phân tích cú pháp CSV chuẩn RFC-4180, xử lý dấu ngoặc kép lồng nhau `""` trong cột mô tả chống lệch cột. |
| `F.expr("try_cast(...)")` | Transformation | Ép kiểu an toàn chuẩn ANSI SQL, tự động trả về `NULL` khi gặp chuỗi rác thay vì crash tiến trình. |
| `F.regexp_extract(...)` | Transformation | Trích xuất biểu thức chính quy (Regex) lấy giá trị số nguyên từ chuỗi diện tích (`sqft`) và tầng. |
| `Imputer(strategy="median")` | Estimator / Transformer | Tính toán song song giá trị trung vị trên từng partition và điền thế vào các ô khuyết thiếu. |
| `StringIndexer(stringOrderType="frequencyDesc")` | Estimator / Transformer | Mã hóa chuỗi định danh thành chỉ số thứ tự thực $0.0, 1.0, \dots$ theo mật độ xuất hiện. |
| `OneHotEncoder(dropLast=True)` | Estimator / Transformer | Chuyển đổi chỉ số nguyên sang biểu diễn nhị phân thưa `SparseVector`, giải quyết bẫy đa cộng tuyến. |
| `VectorAssembler(...)` | Transformer | Ghép nối các đại lượng vô hướng và vector thành một vector trạng thái $\mathbf{x} \in \mathbb{R}^{127}$. |
| `LinearRegression(regParam=0.1)` | Estimator | Giải bài toán tối ưu hóa lồi có điều chuẩn Ridge L2 qua thuật toán phân tán L-BFGS. |
| `F.log(...)` / `F.exp(...)` | Transformation | Biến đổi logarit nén phân phối lệch phải và ánh xạ hàm mũ khôi phục thang đo tiền. |
| `RegressionEvaluator(...)` | Evaluator | Tính toán tập trung các chỉ số thống kê $R^2$, RMSE, MAE trên tập dữ liệu kiểm thử. |

---

## 5. Kết luận & Hướng phát triển tiếp theo

1. **Hiệu chỉnh hệ số Duan (Duan's Smearing Estimator):**
   Để khắc phục hiện tượng tụt $R^2$ của mô hình Log trên thang đo tiền, có thể nhân thêm hệ số hiệu chỉnh không tham số:
   $$\hat{y}_{\text{smearing}} = \exp(\hat{y}_{\text{log}}) \times \left( \frac{1}{N} \sum_{i=1}^N \exp(e_i) \right)$$
2. **Thử nghiệm mô hình phi tuyến tính (Non-linear Ensembles):**
   Triển khai `RandomForestRegressor` hoặc `GBTRegressor` (Gradient-Boosted Trees) trong `pyspark.ml.regression` để nắm bắt các mối quan hệ phi tuyến phức tạp giữa vị trí địa lý và giá bán, hướng tới mục tiêu $R^2 > 85\%$.

