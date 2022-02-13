import sys
from pyspark.sql import SparkSession

input_file = sys.argv[1]
output_file = sys.argv[2]
spark = SparkSession.builder.appName("Page_Ranking").getOrCreate()
links = spark.read.text(input_file)
llist = links.collect()
for i in range(10):
    print(llist[i])
