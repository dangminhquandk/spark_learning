import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.profiler import profile_block


from pyspark.sql import SparkSession
from pyspark.sql import functions as F 
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType
)

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Self_Day1_Mastery")
        .master("local[4]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

# StructField(tên_cột, data_type, nullable)


# Define Schema
schema = StructType([
    StructField("account_id", StringType(), False),
    StructField("holder_name", StringType(), True),
    StructField("account_type", StringType(), True),
    StructField("balance", DoubleType(), True),
    StructField("city", StringType(), True),
    StructField("risk_score", IntegerType(), True)
])

if __name__ == "__main__":
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    with profile_block("Đọc 200 tài khoản từ CSV "):
        df_accounts = (
            spark.read
            .option("header", True)
            .schema(schema)
            .csv("data/raw/accounts.csv")
        )
        print(">> 10 dòng đầu tiên trong bảng:")
        df_accounts.show(10, truncate=False)
        print(">> Tổng số dòng :", df_accounts.count())

    with profile_block("Lọc VIP tại HCM"):
        hcm_vip_df = (
            df_accounts
            .select("account_id", "holder_name", "balance", "city")
            .filter((F.col("balance") >= 10000.0) & (F.col("city") == "HCM"))
            .orderBy(F.col("balance").desc())
        )
        print(">> Danh sách khách hàng VIP hcm:")
        hcm_vip_df.show(truncate=False)

    with profile_block("Phân loại mức độ rủi ro"):
        df_classified = df_accounts.withColumn(
            "risk_level",
            F.when(F.col("risk_score") >= 70, "HIGH")
            .when(F.col("risk_score") >= 40, "MEDIUM")
            .otherwise("LOW")
        )
        print(">> 10 tài khoản sau khi gán nhãn risk_level:")
        df_classified.select("account_id", "holder_name", "risk_score", "risk_level").show(10)

    spark.stop()

