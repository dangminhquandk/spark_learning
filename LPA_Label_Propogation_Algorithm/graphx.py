from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf
from pyspark import SparkContext
import hashlib
from graphframes import GraphFrame
from pyspark.sql.functions import desc

class LPA():

    def __init__(self):
        self.spark = (
            SparkSession.builder
            .appName('LPA_Graph_Analysis')
            .config("spark.jars.packages", "graphframes:graphframes:0.8.4-spark3.5-s_2.13")
            .getOrCreate()
        )

    def graphx(self):
        from pathlib import Path
        base_dir = Path(__file__).resolve().parent
        data_path = str(base_dir / 'results_new' / 'data-00000-of-00010.csv')
        self.df = self.spark.read.option("header", "true").csv(data_path)
        # print(self.df.show(n=5))

        self.df = self.df.dropna()
        self.rdd = self.df.select("url","mention").rdd.flatMap(lambda x: x).distinct()
        # print(self.rdd.take(5))

        def hashnode(x):
            return hashlib.sha1(x.encode("UTF-8")).hexdigest()[:8]

        hashnode_udf = udf(hashnode)

        vertices = self.rdd.map(lambda x: (hashnode(x), x)).toDF(["id", "url"])

        vertices.show(5)

        edges = self.df.select("url", "mention") \
            .withColumn("src", hashnode_udf("url")) \
            .withColumn("dst", hashnode_udf("mention")) \
            .select("src", "dst")

        edges.show(5)

        self.graph = GraphFrame(vertices, edges)
        # print(self.graph)
        print('communities are ')
        self.communities = self.graph.labelPropagation(maxIter=2)

        print(self.communities.persist().show(10))
        print(self.communities.sort(desc("label")).show(50))
        output_path = str(base_dir / "communities")
        self.communities.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
        print("There are " + str(self.communities.select('label').distinct().count()) + " communities in sample graph.")

        print(self.graph.inDegrees.join(vertices, on="id") \
            .orderBy("inDegree", ascending=False).show(10))

        print(self.graph.stronglyConnectedComponents(maxIter=2).select('url','component').show(20))

if __name__ == '__main__':
    lpa = LPA()
    lpa.graphx()