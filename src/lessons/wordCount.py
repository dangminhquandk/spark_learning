from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("WordCountSQL").getOrCreate()

# Đọc file
df = spark.read.text("input.txt")


result = (
    df.select(F.explode(F.split(df.value, ",")).alias("word"))
    .select(F.upper(F.trim(F.col("word"))).alias("word"))
    .filter(F.col("word") != "")
    .groupBy("word")
    .count()
)

result.show()
input("Enter...")
spark.stop()
