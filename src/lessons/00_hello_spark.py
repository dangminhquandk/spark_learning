# ==========================================
# BÀI 0: LÀM QUEN TỪ CON SỐ 0 TRÒN TRĨNH
# ==========================================

# 1. Nhập công cụ Spark vào script
from pyspark.sql import SparkSession

# 2. Khởi động "Động cơ" Spark
print("Đang khởi động Spark...")
spark = SparkSession.builder.appName("Tu_Con_So_0").getOrCreate()
print("Spark đã khởi động xong!")

# 3. Dừng động cơ
spark.stop()
