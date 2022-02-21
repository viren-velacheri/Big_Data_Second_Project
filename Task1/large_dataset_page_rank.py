from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, collect_list, lower, col
import sys
input_file = sys.argv[1]
output_file = sys.argv[2]
spark = SparkSession.builder.appName("Page_Ranking").getOrCreate()
links = spark.read.csv(input_file, comment="#", sep=r'\t').toDF("Page", "Neighbor").select(lower(col('Page')).alias('Page'), lower(col('Neighbor')).alias('Neighbor')).filter("(Neighbor NOT LIKE '%:%' OR Neighbor LIKE '%category:%') AND (Page NOT LIKE '%:%' OR Page LIKE '%category:%')").groupBy("Page").agg(collect_list("Neighbor"))
ranks = links.select("Page").distinct().withColumn("Rank", lit(1))
# links = links.rdd
num_repartitions = 1
iterations = 10
for i in range(iterations):
   contribs = links.join(ranks, ['Page']).rdd.flatMap(lambda x: [(y, x[2]/len(x[1])) for y in x[1]] + [(x[0],0)])
   ranks = contribs.reduceByKey(lambda x,y: x + y).mapValues(lambda x: 0.15 + 0.85 * x).toDF(["Page", "Rank"]).join(links,'Page','leftsemi')

ranks.sort("Rank","Page").write.option("header", True).csv(output_file)

spark.stop()
