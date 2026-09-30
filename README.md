# Vietnam E-commerce Knowledge Graph (VECOM)
Dự án xây dựng **Đồ thị tri thức kinh tế (Economic Knowledge Graph)** phân tích xu hướng thương mại điện tử Việt Nam dựa trên báo cáo VECOM, sử dụng **Lambda Architecture** và **Data Lakehouse**.

## 🏗 Kiến trúc Hệ thống (System Architecture)

Hệ thống được thiết kế theo chuẩn Lambda Architecture kết hợp Lakehouse, tập trung vào khả năng mở rộng xử lý phân tán với Apache Spark & GraphFrames:

```text
spark_learning/
├── data/                  # 🗄️ Tầng lưu trữ Data Lakehouse
│   ├── raw/               # (Bronze) Dữ liệu thô (Báo cáo VECOM, CSV, API)
│   ├── silver/            # (Silver) Dữ liệu đã làm sạch & chuẩn hóa (Parquet/Delta)
│   └── gold/              # (Gold) Knowledge Graph (Vertices, Edges) & Bảng tổng hợp
├── src/                   # ⚙️ Tầng xử lý (Compute Layer)
│   ├── config/            # Cấu hình hệ thống (Spark, DB, Constants)
│   ├── etl/               # Pipeline ETL (Extract, Transform, Load) -> Batch Layer
│   ├── graph/             # Động cơ GraphFrames tính toán PageRank, LPA, Motif
│   ├── dashboard/         # Logic phân tích & trực quan hóa (Serving Layer)
│   ├── utils/             # Tiện ích chung (Profiler, Logger)
│   └── lessons/           # Khu vực học tập & thực hành từng bước (Day 1, Day 2...)
├── notebooks/             # 📓 Phân tích dữ liệu tương tác (EDA & Jupyter)
├── docker-compose.yml     # Khởi tạo môi trường Docker (Spark, GraphFrames, Jupyter)
└── Dockerfile             # Định nghĩa image
```

## 🚀 Luồng dữ liệu (Data Pipeline)

1. **ETL (Batch Layer):** 
   - `src/etl/01_extract_vecom.py`: Trích xuất các thực thể (Địa phương, Sàn TMĐT, Chỉ số) từ dữ liệu thô.
   - Trích xuất mối quan hệ (Cạnh) giữa các thực thể (VD: Dòng chảy hàng hóa, tác động chính sách).
   - Lưu trữ dữ liệu về Data Lake (`data/silver/`).
2. **Graph Processing (Graph Engine Layer):**
   - Nạp Vertices và Edges vào `GraphFrame` (`src/graph/economic_graph.py`).
   - Khai phá các chuỗi liên kết kinh tế (Motif Finding).
   - Tính toán chỉ số mức độ tập trung / sức ảnh hưởng kinh tế của các tỉnh thành (PageRank).
3. **Serving & Dashboard:**
   - Dữ liệu kết quả từ Graph được xuất ra `data/gold/`.
   - Kết nối với Dashboard (Streamlit / Superset) để trực quan hóa Đồ thị tri thức và Bảng xếp hạng.

## 🛠 Cách chạy ứng dụng

```bash
# Khởi động môi trường phân tích trong Docker
docker compose up -d

# Truy cập Spark UI:
# http://localhost:4040
```
