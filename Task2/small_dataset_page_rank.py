from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, collect_list
import sys

# read in arguments
input_file = sys.argv[1]
output_file = sys.argv[2]

# create SparkSession
spark = SparkSession.builder.appName("Page_Ranking").getOrCreate()

# control the number of partitions (we tried both 30 and 60 here)
num_repartitions = 60

# read in dataset from given input path and create Dataframe of Page -> Neighbor where Neighbor is list of outlinks for Page
# repartition links into num_repartitions partitions
links = spark.read.csv(input_file, comment="#", sep=r'\t').toDF("Page", "Neighbor").groupBy("Page").agg(collect_list("Neighbor")).repartition(num_repartitions)

# create Dataframe of all Pages -> 1 to initialize page rank for each page
# repartition ranks into num_repartitions partitions
ranks = links.select("Page").distinct().withColumn("Rank", lit(1)).repartition(num_repartitions)

# run page rank algorithm for 10 iterations
iterations = 10
for i in range(iterations):
   # join pages on their current ranks and then calculate the contribution to each of a page's neighbors
   # include original source page with a contribution of 0 to ensure it is assigned a rank at some point
   # create Dataframe of contributions each page receives in the format of Page -> Contribution
   contribs = links.join(ranks, ['Page']).rdd.flatMap(lambda x: [(y, x[2]/len(x[1])) for y in x[1]] + [(x[0],0)])
   
   # add up contributions by common page and follow formula to calculate new rank for the page
   ranks = contribs.reduceByKey(lambda x,y: x + y).mapValues(lambda x: 0.15 + 0.85 * x).toDF(["Page", "Rank"])

# sort output by Rank ascending and then by Page ascending before combining all partitions into 1
# write out output as a csv file to the given output path
ranks.sort("Rank", "Page").coalesce(1).write.option("header", True).csv(output_file)

spark.stop()
