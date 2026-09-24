import os 
import sys 


from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .appName("First_Principles_Partitions") \
        .master("local[4]") \
        .getOrCreate()


rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5, 6, 7, 8], 4)

print(">> Tổng số partitions:", rdd.getNumPartitions())

nhan_ba = rdd.map(lambda x: x * 3)

# print(">> Bien nhan_ba la:", nhan_ba)

print(">> Dữ liệu nằm trong 4 khối:", nhan_ba.glom().collect())

print(">> Cây phả hệ (Lineage) của nhan_ba:")
print(nhan_ba.toDebugString().decode("utf-8"))

input("Nhấn Enter....")
spark.stop()