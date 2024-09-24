from pyspark import SparkConf, SparkContext
import collections
import os

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")
data_folder = os.getenv('DATA_FOLDER')

# Create a RDD from the file
lines = sc.textFile(data_folder + "/ml-100k/u.data")

# Extract the third column from the RDD which is the rating
ratings = lines.map(lambda x: x.split()[2])

# Count the number of times each rating occurs
result = ratings.countByValue()

# Sort the results by the key
sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
