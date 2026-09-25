"""NGÀY 1: NỀN TẢNG PYSPARK DATAFRAME, ÁNH XẠ SQL & BẢN CHẤT PHÂN TÁN
Mục tiêu:
  1. Cấu hình SparkSession an toàn, tối ưu trên Apple Silicon (Mac M3).
  2. Ánh xạ trực tiếp từ tư duy SQL sang PySpark DataFrame API.
  3. Hiểu cơ chế Lazy Evaluation, DAG, Narrow vs Wide Transformations (Shuffle).
  4. Đo lường hiệu năng: Thời gian chạy (Execution Time) & Biến thiên RAM (Memory Profiling).
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

from src.utils.profiler import profile_block


def create_spark_session() -> SparkSession:
    """Khởi tạo SparkSession tối ưu cho kiến trúc CPU Apple Silicon (Mac M3)."""
    return (
        SparkSession.builder.appName("Day1_PySpark_Foundations")
        .master("local[4]")  # 4 cores phù hợp cho luồng phân tán cục bộ trên M3
        .config("spark.driver.memory", "2g")
        .config(
            "spark.sql.shuffle.partitions", "4"
        )  # Giảm từ mặc định 200 xuống 4 để tránh shuffle overhead với data nhỏ
        .config("spark.default.parallelism", "4")
        .getOrCreate()
    )


def demo_sql_vs_pyspark(spark: SparkSession):
    """Phần 1: Chuyển dịch tư duy SQL sang PySpark DataFrame API."""
    print("\n" + "=" * 60)
    print("PHẦN 1: ÁNH XẠ TRỰC TIẾP SQL -> PYSPARK DATAFRAME")
    print("=" * 60)

    # Định nghĩa Schema tường minh (Hardware-friendly, tránh chi phí Spark phải scan để đoán kiểu)
    schema = StructType(
        [
            StructField("account_id", StringType(), False),
            StructField("holder_name", StringType(), True),
            StructField("account_type", StringType(), True),
            StructField("balance", DoubleType(), True),
            StructField("city", StringType(), True),
            StructField("risk_score", IntegerType(), True),
        ]
    )

    data = [
        ("ACC_01", "Nguyen Van An", "Personal", 15000.0, "Ha Noi", 10),
        ("ACC_02", "Tran Thi Binh", "Personal", 45000.0, "Da Nang", 25),
        ("ACC_03", "Le Van Cuong", "Business", 250000.0, "HCM", 85),
        ("ACC_04", "Pham Minh Duc", "Personal", 8000.0, "Ha Noi", 60),
        ("ACC_05", "Hoang Thi Em", "Personal", 12000.0, "HCM", 70),
        ("ACC_06", "Cong Ty Alpha", "Business", 850000.0, "HCM", 15),
        ("ACC_07", "Vu Van Giang", "Personal", 3000.0, "Da Nang", 40),
    ]

    with profile_block("Khởi tạo DataFrame với Schema tường minh"):
        df_accounts = spark.createDataFrame(data, schema=schema)
        df_accounts.show(truncate=False)

    # 1.1 SELECT & WHERE
    # SQL tương đương:
    # SELECT account_id, holder_name, balance, city FROM accounts WHERE balance >= 10000 AND city = 'HCM';
    with profile_block("Truy vấn Lọc (Filter/Where) & Chiếu (Select)"):
        hcm_high_balance_df = (
            df_accounts.select("account_id", "holder_name", "balance", "city")
            .filter((F.col("balance") >= 10000.0) & (F.col("city") == "HCM"))
            .orderBy(F.col("balance").desc())
        )
        print(">> Kết quả lọc tài khoản số dư >= 10,000 tại HCM:")
        hcm_high_balance_df.show()

    # 1.2 CASE WHEN (Phân loại rủi ro)
    # SQL: CASE WHEN risk_score >= 70 THEN 'HIGH' WHEN risk_score >= 30 THEN 'MEDIUM' ELSE 'LOW' END
    with profile_block("Tạo cột mới có điều kiện (withColumn + F.when)"):
        classified_df = df_accounts.withColumn(
            "risk_level",
            F.when(F.col("risk_score") >= 70, "HIGH (RỦI RO CAO)")
            .when(F.col("risk_score") >= 30, "MEDIUM (TRUNG BÌNH)")
            .otherwise("LOW (AN TOÀN)"),
        )
        classified_df.select(
            "account_id", "holder_name", "risk_score", "risk_level"
        ).show(truncate=False)

    # 1.3 GROUP BY & AGGREGATE
    # SQL: SELECT city, count(*) as total_accounts, avg(balance) as avg_balance, max(risk_score) as max_risk
    #      FROM accounts GROUP BY city HAVING total_accounts >= 2;
    with profile_block("Gom nhóm & Tổng hợp (GroupBy + Agg + Filter)"):
        city_summary_df = (
            df_accounts.groupBy("city")
            .agg(
                F.count("account_id").alias("so_tai_khoan"),
                F.round(F.avg("balance"), 2).alias("so_du_trung_binh"),
                F.max("risk_score").alias("diem_rui_ro_cao_nhat"),
            )
            .filter(F.col("so_tai_khoan") >= 2)
            .orderBy(F.col("so_du_trung_binh").desc())
        )
        print(">> Thống kê tổng hợp theo Thành phố:")
        city_summary_df.show()

    # 1.4 WINDOW FUNCTION (Xếp hạng & Luỹ kế)
    # SQL: DENSE_RANK() OVER (PARTITION BY city ORDER BY balance DESC)
    with profile_block("Hàm Cửa Sổ (Window Function - Rank theo Thành Phố)"):
        window_spec = Window.partitionBy("city").orderBy(F.col("balance").desc())
        ranked_df = df_accounts.withColumn(
            "rank_trong_tp", F.dense_rank().over(window_spec)
        )
        ranked_df.select("city", "rank_trong_tp", "account_id", "balance").show()

    return df_accounts


def demo_joins(spark: SparkSession, df_accounts):
    """Phần 2: Các phép toán JOIN - Nền tảng kết nối Đỉnh (Vertices) và Cạnh (Edges)."""
    print("\n" + "=" * 60)
    print("PHẦN 2: CÁC DẠNG JOIN PHÂN TÁN (INNER, LEFT, SEMI, ANTI)")
    print("=" * 60)

    # Bảng giao dịch chuyển khoản (sẽ là Edges trong GraphFrame sau này)
    tx_schema = StructType(
        [
            StructField("tx_id", StringType(), False),
            StructField("src_acc", StringType(), False),  # Tài khoản gửi
            StructField("dst_acc", StringType(), False),  # Tài khoản nhận
            StructField("amount", DoubleType(), False),
        ]
    )
    tx_data = [
        ("TX_01", "ACC_01", "ACC_02", 5000.0),
        ("TX_02", "ACC_03", "ACC_04", 45000.0),
        ("TX_03", "ACC_04", "ACC_05", 44000.0),
        ("TX_04", "ACC_05", "ACC_03", 43000.0),
        ("TX_05", "ACC_99", "ACC_01", 1000.0),  # ACC_99 không nằm trong bảng accounts!
    ]
    df_tx = spark.createDataFrame(tx_data, schema=tx_schema)

    # 2.1 INNER JOIN: Tìm giao dịch có đầy đủ thông tin tài khoản gửi
    with profile_block("Phép toán INNER JOIN (Tài khoản gửi)"):
        inner_df = df_tx.join(
            df_accounts, df_tx.src_acc == df_accounts.account_id, how="inner"
        ).select(
            "tx_id",
            "src_acc",
            "holder_name",
            "dst_acc",
            "amount",
            "balance",
        )
        inner_df.show()

    # 2.2 LEFT ANTI JOIN: Tìm các giao dịch có tài khoản gửi bất thường (không nằm trong danh sách khách hàng)
    with profile_block("LEFT ANTI JOIN: Phát hiện tài khoản lạ ngoài hệ thống"):
        unknown_acc_tx = df_tx.join(
            df_accounts, df_tx.src_acc == df_accounts.account_id, how="left_anti"
        )
        print(">> Giao dịch xuất phát từ tài khoản KHÔNG TỒN TẠI trong bảng khách hàng:")
        unknown_acc_tx.show()


def demo_spark_internals(df_accounts):
    """Phần 3: Khám phá Catalyst Optimizer & Kế hoạch thực thi (Logical vs Physical Plan)."""
    print("\n" + "=" * 60)
    print("PHẦN 3: BẢN CHẤT SPARK INTERNALS (LAZY EVALUATION & EXECUTION PLAN)")
    print("=" * 60)

    # Tạo một chuỗi các biến đổi (Chưa hề kích hoạt tính toán trên CPU/RAM thực tế!)
    unexecuted_transformation = (
        df_accounts.filter(F.col("balance") > 5000)
        .withColumn("balance_after_tax", F.col("balance") * 0.9)
        .groupBy("account_type")
        .agg(F.sum("balance_after_tax").alias("tong_sau_thue"))
        .filter(F.col("tong_sau_thue") > 20000)
    )

    print(">> Đọc cây tối ưu hóa của Spark bằng explain():")
    # In ra:
    # 1. Parsed Logical Plan (Cú pháp thuần)
    # 2. Analyzed Logical Plan (Kiểm tra kiểu dữ liệu & schema)
    # 3. Optimized Logical Plan (Catalyst đẩy Filter xuống trước khi tính toán - Predicate Pushdown)
    # 4. Physical Plan (Kế hoạch thực thi vật lý: HashAggregate, Exchange / Shuffle)
    unexecuted_transformation.explain(extended=True)

    print("\n>> KÍCH HOẠT ACTION ĐỂ TÍNH TOÁN:")
    with profile_block("Kích hoạt Action `show()`"):
        unexecuted_transformation.show()


def main():
    print("🚀 BẮT ĐẦU BUỔI HỌC NGÀY 1: NỀN TẢNG PYSPARK & TƯ DUY PHÂN TÁN 🚀")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")  # Tắt bớt log INFO dài dòng

    try:
        df_accounts = demo_sql_vs_pyspark(spark)
        demo_joins(spark, df_accounts)
        demo_spark_internals(df_accounts)
        print("\n✅ HOÀN THÀNH TẤT CẢ NỘI DUNG THỰC HÀNH NGÀY 1!")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
