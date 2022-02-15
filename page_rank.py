from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import sys
input_file = sys.argv[1]
output_file = sys.argv[2]
spark = SparkSession.builder.appName("Page_Ranking").getOrCreate()
links = spark.read.csv(input_file, comment="#", sep=r'\t').toDF("Page", "Neighbor")
ranks = links.select("Page").distinct().withColumn("Rank", lit(1))
# links = links.rdd
iterations = 1
for i in range(iterations):
    contribs = links.join(ranks, ['Page']).rdd.flatMap(lambda x: (x[0], x[1], x[2])).collect()
    print(contribs)
