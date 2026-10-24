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
    """Tạo SparkSession"""
    return (
        SparkSession.builder.appName("spark_test")
        .master("local[4]") 
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")  # Giảm từ 200 (mặc định) xuống 4 để tránh shuffle overhead với dữ liệu nhỏ
        .config("spark.default.parallelism", "4")
        .getOrCreate()
    )
 

def demo_sql_vs_pyspark(spark: SparkSession):

    # Định nghĩa Schema
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

    with profile_block("Tạo DataFrame với Schema"):
        df_accounts = spark.createDataFrame(data, schema=schema)
        df_accounts.show(truncate=False)

    # SELECT account_id, holder_name, balance, city FROM accounts WHERE balance >= 10000 AND city = 'HCM';
    with profile_block("Filter/Where & Select"):
        hcm_high_balance_df = (
            df_accounts.select("account_id", "holder_name", "balance", "city")
            .filter((F.col("balance") >= 10000.0) & (F.col("city") == "HCM"))
            .orderBy(F.col("balance").desc())
        )
        print(">> Kết quả lọc tài khoản số dư >= 10,000 tại HCM:")
        hcm_high_balance_df.show()

    # SQL: CASE WHEN risk_score >= 70 THEN 'HIGH' WHEN risk_score >= 30 THEN 'MEDIUM' ELSE 'LOW' END
    with profile_block("Tạo cột mới có điều kiejen"):
        classified_df = df_accounts.withColumn(
            "risk_level",
            F.when(F.col("risk_score") >= 70, "RỦI RO CAO")
            .when(F.col("risk_score") >= 30, "TRUNG BÌNH")
            .otherwise("AN TOÀN"),
        )
        classified_df.select(
            "account_id", "holder_name", "risk_score", "risk_level"
        ).show(truncate=False)

    # SQL: SELECT city, count(*) as total_accounts, avg(balance) as avg_balance, max(risk_score) as max_risk
    #      FROM accounts GROUP BY city HAVING total_accounts >= 2;
    with profile_block("Gom nhóm & Tổng hợp"):
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
        print(">> Thống kê theo Thành phố:")
        city_summary_df.show()

    # DENSE_RANK() OVER (PARTITION BY city ORDER BY balance DESC): nếu bằng nhau thì cùng stt, ko bị gap
    with profile_block("Rank theo Thành Phố"):
        window_spec = Window.partitionBy("city").orderBy(F.col("balance").desc())
        ranked_df = df_accounts.withColumn(
            "rank_trong_tp", F.dense_rank().over(window_spec)
        )
        ranked_df.select("city", "rank_trong_tp", "account_id", "balance").show()

    return df_accounts


def demo_joins(spark: SparkSession, df_accounts):
  
    # Bảng giao dịch chuyển khoản
    tx_schema = StructType(
        [
            StructField("tx_id", StringType(), False),
            StructField("src_acc", StringType(), False),  # Tk gửi
            StructField("dst_acc", StringType(), False),  # TK nhận
            StructField("amount", DoubleType(), False),
        ]
    )
    tx_data = [
        ("TX_01", "ACC_01", "ACC_02", 5000.0),
        ("TX_02", "ACC_03", "ACC_04", 45000.0),
        ("TX_03", "ACC_04", "ACC_05", 44000.0),
        ("TX_04", "ACC_05", "ACC_03", 43000.0),
        ("TX_05", "ACC_99", "ACC_01", 1000.0), 
    ]
    df_tx = spark.createDataFrame(tx_data, schema=tx_schema)


    with profile_block("INNER JOIN"):
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
    with profile_block("LEFT ANTI JOIN:"):
        unknown_acc_tx = df_tx.join(
            df_accounts, df_tx.src_acc == df_accounts.account_id, how="left_anti"
        )
        print(">> Giao dịch xuất phát từ tài khoản ko có trong bảng khách hàng:")
        unknown_acc_tx.show()


def demo_spark_internals(df_accounts):

    # Tạo một chuỗi các biến đổi
    unexecuted_transformation = (
        df_accounts.filter(F.col("balance") > 5000)
        .withColumn("balance_after_tax", F.col("balance") * 0.9)
        .groupBy("account_type")
        .agg(F.sum("balance_after_tax").alias("tong_sau_thue"))
        .filter(F.col("tong_sau_thue") > 20000)
    )

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
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        df_accounts = demo_sql_vs_pyspark(spark)
        demo_joins(spark, df_accounts)
        demo_spark_internals(df_accounts)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
