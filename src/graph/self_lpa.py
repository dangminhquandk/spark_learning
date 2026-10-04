# 1
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Self_LPA_Mastery")
    .config("spark.jars.packages", "graphframes:graphframes:0.8.4-spark3.5-s_2.13")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 2
import hashlib
from pyspark.sql.functions import udf
from graphframes import GraphFrame

df = spark.read.option("header", "true").csv("LPA_Label_Propogation_Algorithm/results_new/data-00000-of-00010.csv")
df = df.dropna()

rdd = df.select("url", "mention").rdd.flatMap(lambda x: x).distinct()

def hashnode(x):
    return hashlib.sha1(x.encode("UTF-8")).hexdigest()[:8]

vertices = rdd.map(lambda x: (hashnode(x), x)).toDF(["id", "url"])

print("Veticels table:")
vertices.show(5, truncate = False)

# 3
hashnode_udf = udf(hashnode)

edges = (
    df.select("url", "mention")
    .withColumn("src", hashnode_udf("url"))
    .withColumn("dst", hashnode_udf("mention"))
    .select("src", "dst")
)

print("Edges table")
edges.show(5)

graph = GraphFrame(vertices, edges)
print(">> Done")