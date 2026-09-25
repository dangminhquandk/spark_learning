from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .appName("DataFrame_Shuffle_Discovery") \
        .master("local[4]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

data = [
        ("An", "Ha Noi"),
        ("Binh", "Da Nang"),
        ("Cuong", "HCM"),
        ("Dung", "Ha Noi"),
        ("Em", "HCM"),
        ("Giang", "Ha Noi")
]

columns = ["Name", "City"]

df = spark.createDataFrame(data, columns)

print(">> Schema:")
df.printSchema()

print(">> Content:")
df.show()

print(">> Số lượng Partitions:", df.rdd.getNumPartitions())

print(">> phân bổ:", df.rdd.glom().collect())

print(">> KQ đếm số người theo thành phố:")
city_count_df = df.groupBy("City").count()

city_count_df.show()

input("Nhap Enter....")

spark.stop()