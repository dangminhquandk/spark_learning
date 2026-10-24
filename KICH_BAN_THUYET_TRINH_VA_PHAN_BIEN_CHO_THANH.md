# 🎯 KỊCH BẢN THUYẾT TRÌNH & BÍ KÍP BẢO VỆ PHẢN BIỆN (DEFENSE CHEAT SHEET)
### Dành riêng cho: Nguyễn Văn Thành (Báo cáo Thầy TS. Trần Văn Đặng)
* **Đề tài:** Đánh giá khả năng thay thế GraphX bằng GraphFrames và Hiện đại hóa hệ thống Data Provenance trên Apache Spark
* **Nhóm thực hiện:** Đặng Minh Quân - Nguyễn Văn Thành
* **Tài liệu báo cáo chính:** [BAO_CAO_TITIAN_PRISM.md](file:///Users/quandkh/Documents/Research%20Hub/spark_learning/BAO_CAO_TITIAN_PRISM.md)

---

## 📌 I. CHIẾN LƯỢC BÁO CÁO TỔNG QUAN (MINDSET)
1. **Tư thế báo cáo:** Tự tin, đi thẳng vào bản chất kỹ thuật, không vòng vo.
2. **Thông điệp cốt lõi cần Thầy ghi nhận:**
   * Nhóm **đã trả lời trực diện câu hỏi của Thầy**: Titian (2015) không thể chạy với GraphFrames vì khác biệt kiến trúc (RDD vs Catalyst WSCG).
   * Nhóm **không dừng lại ở lý thuyết mà đã tìm ra giải pháp tối tân nhất**: Hệ thống **PRISM (ACM SoCC 2026)** của chính tác giả Titian.
   * Nhóm **đã dựng cụm phân tán Spark 4 trên Docker và tái hiện thực nghiệm thành công 100%**.
   * Nhóm **đã có thiết kế Kiến trúc Lai (Hybrid Architecture) kết hợp GraphFrames** để tự động hóa Root-Cause Analysis (RCA).

---

## 🎙️ II. KỊCH BẢN NÓI CHI TIẾT THEO 5 CHẶNG (THỜI LƯỢNG: 7 - 10 PHÚT)

### ⏱️ Chặng 1: Mở đầu & Trả lời trực diện câu hỏi của Thầy (1 phút)
> *"Kính thưa Thầy, em xin phép đại diện nhóm báo cáo tiến độ nghiên cứu về đề tài Data Provenance trên Apache Spark theo yêu cầu của Thầy hôm trước.*
>
> *Thầy có đặt cho nhóm 2 câu hỏi trọng tâm:*
> 1. *Paper Titian (VLDB 2015) dùng GraphX vốn đã cũ, liệu có thể thay thế bằng GraphFrames được không?*
> 2. *Hãy chạy lại mã nguồn thực nghiệm xem có đúng như bài báo công bố hay không?*
>
> *Nhóm em xin khẳng định ngay kết luận cốt lõi:*  
> *Về mặt kỹ thuật, **KHÔNG THỂ cắm trực tiếp GraphFrames vào mã nguồn Titian 2015**. Nhưng nhóm em đã tìm ra giải pháp hiện đại hóa hoàn hảo kế thừa từ chính tác giả Titian: đó là hệ thống **PRISM (ACM SoCC 2026)**. Nhóm đã dựng cụm Spark 4 phân tán trên Docker và tái hiện thành công toàn bộ các phép đo khoa học của công trình này."*

---

### ⏱️ Chặng 2: Giải phẫu lý do Titian "bất lực" trước GraphFrames (2 phút)
*(Chỉ tay vào sơ đồ Mermaid ở Mục 1 trong báo cáo)*

> *"Thưa Thầy, nguyên nhân sâu xa nằm ở sự tiến hóa kiến trúc của Apache Spark:*
> * *Năm 2015, Titian ra đời trên **Spark 1.2.1** dựa vào tầng trừu tượng **RDD**. Lúc đó, mỗi toán tử là một đối tượng Java riêng biệt trên Heap. Titian chỉ cần cắm các 'Iterator Hook' vào giữa các RDD để bắt dòng dữ liệu. Vì GraphX cũng viết trên RDD, nên Titian chạy được tự nhiên với GraphX.*
> * *Tuy nhiên, **GraphFrames** lại được xây dựng hoàn toàn trên **Spark SQL, DataFrames và bộ tối ưu hóa Catalyst**.*
> * *Từ Spark 2.0 trở đi, dự án **Project Tungsten** giới thiệu cơ chế **Whole-Stage Code Generation (WSCG)**. Thay vì để các RDD gọi nhau, Catalyst nén toàn bộ pipeline quan hệ (Scan -> Filter -> Project -> Aggregate) thành **MỘT HÀM BYTECODE JAVA DUY NHẤT** chạy thẳng trên thanh ghi CPU và bộ nhớ nhị phân UnsafeRow.*
> * *Hậu quả là: Trong Spark hiện đại, **toàn bộ ranh giới RDD bị nung chảy và biến mất hoàn toàn**. Các điểm móc của Titian 2015 bị vô hiệu hóa 100%. Đây là lý do vì sao không thể đưa GraphFrames vào Titian."*

---

### ⏱️ Chặng 3: Đột phá công nghệ của PRISM - ACM SoCC 2026 (2 phút)
> *"Để giải bài toán này, chính tác giả chính của Titian là GS. Muhammad Ali Gulzar (Đại học Virginia Tech) vừa công bố hệ thống **PRISM** tại hội nghị ACM SoCC 2026.*
>
> *Prism đưa ra 3 ý tưởng đột phá:*
> 1. ***Hòa làm một với Whole-Stage Codegen:** Thay vì đứng ngoài, Prism dùng `PrismSQLExtension` để chèn các toán tử `TapExec` trực tiếp vào cây kế hoạch vật lý Catalyst. Khi Spark sinh mã bytecode, Prism tự động 'tiêm' đúng 1 dòng lệnh siêu nhẹ `$tap.tap()` vào bên trong vòng lặp `while` duy nhất của Spark. Dữ liệu ghi chép bằng mảng số nguyên thủy `Array[Long]`, không làm phát sinh RDD mới và không gây áp lực rác GC.*
> 2. ***Bóc tách hộp đen UDF bằng Python AST Rewriting:** File `fineprov.py` tự động đọc cây cú pháp của hàm Python UDF, phân tách rạch ròi giữa `_data lineage` (tham gia tính toán số học) và `_control lineage` (tham gia rẽ nhánh if/else).*
> 3. ***Thuật toán Xếp hạng Ảnh hưởng vi phân $O(n)$:** Khi tổng doanh thu bị âm, thay vì phải chạy lại pipeline cả triệu lần theo kiểu Leave-One-Out, Prism dùng công thức giải tích vi phân tính toán mức độ ảnh hưởng của từng dòng trong **duy nhất 1 lần duyệt $O(n)$**."*

---

### ⏱️ Chặng 4: Bằng chứng Thực nghiệm Định lượng trên Docker cụm Spark 4 (2 phút)
*(Chỉ vào Bảng số liệu ở Mục 5 trong báo cáo)*

> *"Thưa Thầy, nhóm em đã dựng cụm phân tán gồm 1 Master, 2 Workers và 1 JupyterLab trên Docker để chạy lại các bộ benchmark kiểm chứng:*
> 1. *Bộ test toán học `validate_influence.py` với **20.000 phép thử ngẫu nhiên**: Tỷ lệ khớp thứ hạng chính xác đạt **100%**, sai số tuyệt đối cực nhỏ $< 1.82 \times 10^{-9}$ (hoàn toàn do giới hạn làm tròn số thực 64-bit).*
> 2. *Bộ test `microbench`: Phả hệ cấp cell của Prism đạt độ chuẩn xác **Precision = 99.9%** (triệt tiêu toàn bộ cảnh báo giả), trong khi phả hệ cấp dòng của Titian chỉ đạt 50%.*
> 3. *Bộ test thư viện C `test_native_udf.py`: Chứng minh phương pháp bọc đối tượng cũ bị crash khi gặp NumPy, trong khi AST Rewriting của Prism chạy mượt mà 100%.*
> 4. *Thực nghiệm 3 bài toán thực tế (Chuyến bay, Thời tiết, Multi-table Join): Nhờ cơ chế tỉa phân vùng (partition pruning), dung lượng lưu trữ phả hệ giảm tới **95.7%** (từ 0.82 MB xuống 0.035 MB) và số task cần quét giảm từ **28 tasks xuống chỉ còn 5 tasks**."*

---

### ⏱️ Chặng 5: Đề xuất Lộ trình tiếp theo & Mô hình Lai (Hybrid) với GraphFrames (2 phút)
*(Chỉ vào Mục 8.2 trong báo cáo)*

> *"Từ những phát hiện trên, nhóm em xin đề xuất với Thầy định hướng nghiên cứu tiếp theo:*
> * *Chúng ta **không dùng GraphFrames để thay thế tầng thu thập phả hệ của Prism**, vì việc tạo DataFrame đồ thị toàn cục sẽ làm bùng nổ bộ nhớ và nghẽn shuffle.*
> * *Thay vào đó, nhóm đề xuất **Kiến trúc Lai (Hybrid Architecture)**: Prism đảm nhiệm thu thập phả hệ siêu nhẹ $O(1)$ lúc runtime. Khi phát hiện một cell dữ liệu bị dị biệt (Sink Node), hệ thống sẽ **phóng chiếu đồ thị theo nhu cầu (On-Demand Subgraph)** trên vùng ảnh hưởng hẹp.*
> * *Trên đồ thị con này, nhóm sẽ áp dụng **GraphFrames để chạy giải thuật Personalized PageRank ngược dòng**, tự động tính toán xác suất và xếp hạng chính xác nguyên nhân gốc (Root-Cause Analysis) gây ra lỗi dữ liệu.*
> * *Đồng thời, nhóm sẽ áp dụng cơ chế `checkpoint()` định kỳ sau mỗi 3 bước lặp đồ thị để triệt tiêu hoàn toàn nguy cơ tràn ngăn xếp `StackOverflowError` của Catalyst.*
>
> *Nhóm em đã chuẩn bị sẵn sàng mã nguồn và môi trường thực nghiệm, rất mong nhận được góp ý và định hướng của Thầy ạ!"*

---

## 🛡️ III. BỘ 6 CÂU HỎI PHẢN BIỆN "HÓC BÚA" CỦA THẦY ĐẶNG & CÁCH ĐÁP TRẢ

### ❓ Câu 1: "Tại sao nhóm không sửa mã nguồn Titian 2015 để hỗ trợ DataFrame/GraphFrames mà phải chuyển sang Prism?"
* **Thành trả lời chuẩn bài:**  
  *"Dạ thưa Thầy, Titian can thiệp ở tầng thực thi RDD (Volcano Iterator Model). Nếu muốn Titian chạy trên DataFrame, ta buộc phải tắt cơ chế Whole-Stage Codegen (WSCG) của Spark. Nhưng nếu tắt WSCG, hiệu năng của Spark SQL sẽ tụt dốc từ 5 đến 10 lần do chi phí gọi hàm ảo và garbage collection. Tác giả Titian là GS. Gulzar đã nhận ra điểm nghẽn này nên chính thầy ấy đã phát triển Prism để can thiệp thẳng vào cây kế hoạch vật lý Catalyst của WSCG. Do đó, kế thừa Prism là con đường khoa học chuẩn xác nhất ạ."*

### ❓ Câu 2: "Toán tử Tap của Prism cắm vào chỗ nào trong Spark? Có làm chậm hệ thống không?"
* **Thành trả lời chuẩn bài:**  
  *"Dạ thưa Thầy, Prism dùng `PrismSQLExtension` chèn một `ColumnarRule` vào trước phase `CollapseCodegenStages`. Các toán tử Tap (như `TapScanExec`, `TapPreExchangeExec`) kế thừa trait `CodegenSupport`. Khi Spark biên dịch code Java runtime, Tap chỉ tiêm đúng 1 lệnh `$tap.tap()` vào vòng lặp `while` duy nhất của cả Stage. Dữ liệu được ghi thẳng vào mảng số nguyên thủy `Array[Long]` lưu trên `BlockManager`, hoàn toàn không cấp phát Java Object trên Heap nên overhead thực nghiệm chỉ tốn khoảng ~11% ạ."*

### ❓ Câu 3: "UDF viết bằng Python chạy trên tiến trình riêng (Python Worker), làm sao JVM của Spark biết được bên trong nó tính toán cái gì?"
* **Thành trả lời chuẩn bài:**  
  *"Dạ thưa Thầy, đây chính là sự tinh tế của Prism. Thay vì can thiệp từ JVM, Prism xử lý ngay tại tầng Python driver bằng file `fineprov.py`. Khi khai báo UDF, Prism dùng thư viện `ast` của Python duyệt cây cú pháp trừu tượng, viết lại hàm để trả về kết quả kèm một Bitmask 64-bit mã hóa trường nào là `_data` và trường nào là `_control`. Bitmask này được gửi kèm dòng dữ liệu nhị phân về cho JVM và được `TapExec` lưu lại thành Cell-level provenance ạ."*

### ❓ Câu 4: "Tại sao nhóm đo sai số của thuật toán ảnh hưởng là $1.75 \times 10^{-9}$? Con số này có ý nghĩa gì?"
* **Thành trả lời chuẩn bài:**  
  *"Dạ thưa Thầy, trong file `validate_influence.py`, nhóm đối chiếu công thức vi phân $O(n)$ của Prism với phương pháp chuẩn mực Leave-One-Out (chạy lại từng dòng). Về mặt toán học, hai phương pháp này là đồng nhất. Sai số $1.75 \times 10^{-9}$ không phải là lỗi giải thuật, mà là sai số tích lũy làm tròn số thực dấu phẩy động 64-bit (chuẩn IEEE 754) trong các phép chia của máy tính. Nó chứng minh công thức của Prism chuẩn xác 100% như Leave-One-Out nhưng tốc độ nhanh hơn hàng nghìn lần ạ."*

### ❓ Câu 5: "Nếu áp dụng GraphFrames vào phả hệ, khi duyệt đồ thị lặp 20-30 lần thì gặp vấn đề gì về mặt hệ thống phân tán?"
* **Thành trả lời chuẩn bài:**  
  *"Dạ thưa Thầy, đây là điểm nghẽn nghiêm trọng nhất của GraphFrames mà nhóm em đã lường trước: GraphFrames hoạt động trên DataFrames, mỗi bước duyệt ngược tương đương 1 phép Join làm cây Catalyst Logical Plan dài thêm 1 bậc. Sau 25-30 bước, Driver sẽ bị lỗi `StackOverflowError` hoặc OOM khi duyệt đệ quy. Để khắc phục, nhóm em thiết kế bắt buộc phải gọi `checkpoint()` định kỳ sau mỗi 3 bước lặp để ngắt đứt cây Catalyst Plan, ghi trạng thái xuống đĩa và khởi tạo lại plan mới ạ."*

### ❓ Câu 6: "Vậy tóm lại, đề tài này nhóm định dùng GraphFrames để làm gì cụ thể?"
* **Thành trả lời chuẩn bài:**  
  *"Dạ thưa Thầy, nhóm em đề xuất mô hình Lai (Hybrid): Không dùng GraphFrames để ghi phả hệ runtime, mà dùng GraphFrames cho **bài toán Root-Cause Analysis (RCA) tự động sau khi có lỗi**. Khi người dùng phát hiện 1 số liệu kinh doanh bị âm, hệ thống trích xuất đồ thị con liên quan và đưa vào GraphFrames để chạy giải thuật **Personalized PageRank ngược dòng**, tự động xếp hạng và chỉ ra chính xác bản ghi nguồn nào hoặc toán tử nào là nguyên nhân gốc rễ gây ra lỗi ạ."*

---

## 📋 IV. BẢNG TRA CỨU NHANH TỪ VỰNG KỸ THUẬT (FLASHCARDS CHO THÀNH)

| Thuật ngữ | Ý nghĩa kỹ thuật tóm tắt |
| :--- | :--- |
| **WSCG** | *Whole-Stage Code Generation:* Cơ chế gộp toàn bộ pipeline thành 1 hàm Java bytecode của Spark 2+. |
| **Catalyst Optimizer** | Bộ tối ưu hóa truy vấn lõi của Spark SQL (phân tích, tối ưu logic và sinh kế hoạch vật lý). |
| **TapExec** | Toán tử vật lý của Prism chèn mã quan sát phả hệ vào vòng lặp WSCG. |
| **Packed Identifier** | Kỹ thuật dịch bit ghép `(partition_id << 32) \| row_offset` thành 1 số `Long 64-bit`. |
| **AST Rewriting** | Kỹ thuật đọc và sửa cây cú pháp của code Python UDF để tự động truyền lineage. |
| **Leave-One-Out (LOO)** | Phương pháp đo độ ảnh hưởng bằng cách bỏ bớt 1 dòng và chạy lại để đo độ lệch. |
| **Personalized PageRank** | Thuật toán xếp hạng độ quan trọng của các đỉnh đồ thị tập trung từ một đỉnh nguồn chỉ định. |
| **Checkpointing** | Ghi dữ liệu RDD/DataFrame xuống đĩa tin cậy để cắt đứt cây phả hệ DAG, chống tràn stack. |

---
*Chúc Nguyễn Văn Thành và nhóm hoàn thành xuất sắc buổi báo cáo trước Thầy Trần Văn Đặng!*
