# 🚀 Spark Learning & GraphFrames Research Lab

> **Dự án Nghiên cứu & Thực nghiệm Thuật toán Đồ thị Phân tán (Distributed Graph Mining) trên nền tảng Apache Spark & GraphFrames.**  
> **Thành viên:** Đặng Minh Quân (Lead), Thành — **Giảng viên hướng dẫn:** Thầy Đặng.  
> **Nền tảng thực thi:** Apple Silicon (MacBook Air M3, ARM64), Python 3.9, Apache Spark 4.0.4, GraphFrames.

---

## 📌 1. Mục tiêu Nghiên cứu

Dự án được xây dựng với mục tiêu làm chủ từ bản chất phần cứng đến các thuật toán nâng cao trên đồ thị quy mô lớn:

1. **Nền tảng từ Nguyên lý cơ bản (First Principles):** Hiểu thấu đáo cách Spark phân bổ dữ liệu (Partitions), cơ chế thực thi lười biếng (Lazy Evaluation), đồ thị phả hệ (DAG Lineage) và hiện tượng xáo trộn dữ liệu (Shuffle).
2. **Khai phá Đồ thị Phân tán (Graph Mining):** Triển khai và tối ưu hóa bộ thuật toán cốt lõi trên GraphFrames:
   - **LPA (Label Propagation Algorithm):** Phát hiện cộng đồng (Community Detection) trong mạng lưới phức tạp theo cơ chế lan truyền nhãn và biểu quyết đa số.
   - **PageRank:** Xác định độ trung tâm và tầm ảnh hưởng của các nút/dòng tiền.
   - **Connected Components:** Gom cụm và khai phá các phân vùng liên thông.
   - **Motif Finding:** Quét các mẫu hình liên kết và chu trình khép kín.
3. **Kỹ thuật Đo lường & Tối ưu Phần cứng (Profiling & Optimization):** Thiết lập chuẩn đo lường thời gian chạy (Execution Time) và biến thiên RAM (`psutil`), ngăn ngừa tràn bộ đệm JVM và tối ưu hóa bộ nhớ trên chip Apple M3.

---

## 🗺️ 2. Lộ trình Nghiên cứu 7 Ngày (Intensive Bootcamp)

Chi tiết kế hoạch toàn thời gian được mô tả trong [ROADMAP_1_WEEK_INTENSIVE_GRAPHFRAME.md](ROADMAP_1_WEEK_INTENSIVE_GRAPHFRAME.md):

- **Ngày 1:** Nền tảng PySpark, Kiến trúc phân tán (Driver vs Worker), Partitions, Lazy Evaluation & Spark Web UI (`localhost:4040`).
- **Ngày 2:** Khởi tạo GraphFrame Core, Lược đồ Vertices/Edges, Phân tích Bậc đỉnh (Degree Centrality).
- **Ngày 3:** Tìm kiếm mẫu hình (Motif Finding), Bắt chu trình đồ thị và ứng dụng phát hiện bất thường.
- **Ngày 4 (Trọng tâm):** Thuật toán Lan truyền nhãn (**LPA**) & Khảo sát cấu trúc cộng đồng (Community Detection).
- **Ngày 5:** PageRank & Thuật toán tìm đường đi ngắn nhất (BFS).
- **Ngày 6:** Connected Components & Phân cụm mạng lưới quy mô lớn.
- **Ngày 7:** Cắt tỉa nhánh lineage (`checkpoint`), Stress-test dữ liệu lớn và Tổng kết báo cáo.

---

## 📂 3. Cấu trúc Dự án

```text
spark_learning/
├── .agents/                    # Quy chuẩn và chỉ dẫn tác nhân thông minh
│   └── rules/spark.md         # Tiêu chuẩn môi trường venv và tối ưu Spark
├── data/                       # Dữ liệu thử nghiệm cục bộ (được bỏ qua bởi git)
├── notebooks/                  # Sổ tay nghiên cứu Jupyter Notebooks
├── src/                        # Mã nguồn chính
│   ├── utils/
│   │   ├── __init__.py
│   │   └── profiler.py        # Module đo lường thời gian thực thi & RAM
│   ├── day1_dataframe_basics.py # Thực hành Spark DataFrame & Internals
│   └── wordCount.py           # Bài toán kinh điển phân tán
├── start.py                    # Script nhập môn khảo sát Partitions & DAG Web UI
├── ROADMAP_1_WEEK_INTENSIVE_GRAPHFRAME.md # Lộ trình chi tiết 7 ngày
├── requirements.txt            # Danh sách thư viện phụ thuộc
├── .gitignore                  # Cấu hình loại trừ file rác, dữ liệu nặng và venv
└── README.md                   # Tài liệu giới thiệu dự án
```

---

## ⚙️ 4. Hướng dẫn Thiết lập & Chạy thử nghiệm

### Yêu cầu tiên quyết:
- macOS (Apple Silicon M1/M2/M3) hoặc Linux
- Java 17 hoặc Java 21 (đã cấu hình `JAVA_HOME`)
- Python 3.9+ trong môi trường ảo nội bộ `./venv`

### Cài đặt môi trường:
```bash
# 1. Kích hoạt môi trường ảo nội bộ
source venv/bin/activate

# 2. Cài đặt các thư viện cần thiết
pip install -r requirements.txt
```

### Chạy thử nghiệm nhập môn:
```bash
# Khảo sát Partitions và xem DAG Lineage
python start.py
```
Khi chương trình dừng ở bước chờ, mở trình duyệt web truy cập:
👉 **`http://localhost:4040`** để xem giao diện **Spark Web UI & DAG Visualization**.

---

## 📊 5. Chuẩn mực Mã nguồn & Profiling

Mọi tác vụ thuật toán đều được bọc trong bộ đo tài nguyên để phục vụ phân tích học thuật:

```python
from src.utils.profiler import profile_block

with profile_block("Thuật toán Lan truyền nhãn (LPA)"):
    # Code thuật toán Spark / GraphFrame ở đây
    pass
```
*Kết quả sẽ tự động in ra màn hình thời gian thực thi (giây) và mức tăng trưởng RAM (MB).*
