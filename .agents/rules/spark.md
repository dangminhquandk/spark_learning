---
trigger: always_on
---

# QUY CHUẨN DỰ ÁN SPARK & DATA PIPELINE (WORKSPACE)

1. MÔI TRƯỜNG THỰC THI:
   - Luôn sử dụng môi trường ảo nội bộ: `./venv/bin/python` và `./venv/bin/pip`.
   - Không tự ý cài đặt thêm thư viện global vào máy khi chưa được xác nhận.

2. KIẾN TRÚC MÃ NGUỒN & PHÂN QUYỀN FILE:
   - Giữ nguyên cấu trúc thư mục hiện có: mã nguồn đặt tại `src/`, dữ liệu thử nghiệm tại `data/`, sổ tay tại `notebooks/`.
   - Khi tạo script mới, luôn thiết lập cơ chế đo lường thời gian chạy (Execution Time) và mức tiêu thụ tài nguyên bộ nhớ (Memory Profiling).
   - Với các tác vụ Apache Spark / GraphFrames: Luôn kiểm soát cơ chế Lazy Evaluation, tối ưu hóa việc phân vùng (Partitioning) và hạn chế Shuffle dữ liệu không cần thiết.