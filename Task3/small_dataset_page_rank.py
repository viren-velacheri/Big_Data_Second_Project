from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, collect_list
import sys
input_file = sys.argv[1]
output_file = sys.argv[2]
spark = SparkSession.builder.appName("Page_Ranking").getOrCreate()

links = spark.read.csv(input_file, comment="#", sep=r'\t').toDF("Page", "Neighbor").groupBy("Page").agg(collect_list("Neighbor")).persist()
ranks = links.select("Page").distinct().withColumn("Rank", lit(1))
# links = links.rdd
num_repartitions = 1
iterations = 10
for i in range(iterations):
   contribs = links.join(ranks, ['Page']).rdd.flatMap(lambda x: [(y, x[2]/len(x[1])) for y in x[1]] + [(x[0],0)])
   ranks = contribs.reduceByKey(lambda x,y: x + y).mapValues(lambda x: 0.15 + 0.85 * x).toDF(["Page", "Rank"])

ranks.sort("Rank", "Page").coalesce(num_repartitions).write.option("header", True).csv(output_file)

spark.stop()
