import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


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

from src.utils.profiler import profile_block

# Define Schema
schema = StructType([
    StructField("account_id", StringType(), False),
    StructField("holder_name", StringType(), True),
    StructField("account_type", StringType(), True),
    StructField("balance", DoubleType(), True),
    StructField("city", StringType(), True),
    StructField("risk_score", IntegerType(), True)
])

# Raw_data
data = [
    ("ACC_01", "Nguyen Van An", "Personal", 15000.0, "Ha Noi", 10),
    ("ACC_02", "Tran Thi Binh", "Personal", 45000.0, "Da Nang", 25),
    ("ACC_03", "Le Van Cuong", "Business", 250000.0, "HCM", 85),
    ("ACC_04", "Pham Minh Duc", "Personal", 8000.0, "Ha Noi", 60),
    ("ACC_05", "Hoang Thi Em", "Personal", 12000.0, "HCM", 70),
    ("ACC_06", "Cong Ty Alpha", "Business", 850000.0, "HCM", 15),
    ("ACC_07", "Vu Van Giang", "Personal", 3000.0, "Da Nang", 40),
]

if __name__ == "__main__":
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    with profile_block("Initialize DataFrame with Schema"):
        df_accounts = spark.createDataFrame(data, schema = schema)
        df_accounts.show(truncate=False)

    input("Enter.....")
    spark.stop()

