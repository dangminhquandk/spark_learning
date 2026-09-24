from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("WordCountSQL").getOrCreate()

# Đọc file văn bản
df = spark.read.text("input.txt")

# Tách chuỗi theo dấu phẩy thành mảng -> bung mảng thành từng dòng (explode) -> chuẩn hóa -> đếm
result = (
    df.select(F.explode(F.split(df.value, ",")).alias("word"))
    .select(F.upper(F.trim(F.col("word"))).alias("word"))
    .filter(F.col("word") != "")
    .groupBy("word")
    .count()
)

result.show()
input("Nhấn Enter để kết thúc...")
spark.stop()