# ⚡ Apache Spark Research Hub: Graph Mining & Data Provenance

Dự án nghiên cứu chuyên sâu về **Apache Spark Internals**, **Thuật toán Đồ thị Phân tán (GraphFrames / GraphX)** và **Hệ thống Phả hệ Dữ liệu (Data Provenance: Titian VLDB 2015 $\rightarrow$ PRISM ACM SoCC 2026)**.

---

## 📌 Tài liệu Báo cáo & Thuyết trình Trọng tâm

* 📑 **Báo cáo Nghiên cứu & Thực nghiệm Chính quy:**  
  👉 **[BAO_CAO_TITIAN_PRISM.md](./BAO_CAO_TITIAN_PRISM.md)**  
  *(Đánh giá khả năng thay thế GraphX bằng GraphFrames, phân tích giới hạn của Titian trên Catalyst WSCG / Tungsten, và tái hiện thành công hệ thống PRISM ACM SoCC 2026 trên cụm Docker).*

* 🎤 **Kịch bản Thuyết trình & Bộ câu hỏi Phản biện:**  
  👉 **[KICH_BAN_THUYET_TRINH_VA_PHAN_BIEN_CHO_THANH.md](./KICH_BAN_THUYET_TRINH_VA_PHAN_BIEN_CHO_THANH.md)**  
  *(Khung thuyết trình 7 phút 5 giai đoạn kèm 6 cặp câu hỏi phản biện đối kháng chuyên sâu).*

* 📚 **Tài liệu & Paper Nghiên cứu:** Thư mục **[`paper/`](./paper/)** chứa các paper gốc (Titian VLDB 2015, GraphX, RDD, Spark SQL SIGMOD, v.v.).

---

## 🏗 Cấu trúc Dự án

```text
.
├── BAO_CAO_TITIAN_PRISM.md                  # Báo cáo kỹ thuật chi tiết Titian vs PRISM
├── KICH_BAN_THUYET_TRINH_VA_PHAN_BIEN_CHO_THANH.md # Kịch bản báo cáo và phản biện
├── paper/                                   # Các bài báo khoa học quốc tế liên quan
├── src/
│   ├── lessons/                             # Thực hành Spark RDD, DataFrame, Profiling
│   ├── graph/                               # Đồ thị kinh tế VECOM, PageRank, LPA
│   └── utils/                               # Tiện ích đo lường hiệu năng phần cứng
├── LPA_Label_Propogation_Algorithm/         # Thuật toán phát hiện cộng đồng LPA
├── repos/prism-spark-provenance/            # Cụm Docker phân tán và benchmark PRISM
├── docker-compose.yml                       # Khởi động cụm Spark Lab
└── requirements.txt                         # Thư viện phụ thuộc
```

---

## 🚀 Khởi chạy Thực nghiệm

### 1. Môi trường Python cục bộ
```bash
pip install -r requirements.txt
python src/graph/economic_graph.py
```

### 2. Môi trường Cụm Phân tán Docker (PRISM & Spark 4)
```bash
cd repos/prism-spark-provenance
docker compose -f demo/docker-compose.yml up --build
```
