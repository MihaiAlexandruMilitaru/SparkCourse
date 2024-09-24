from pyspark.sql import SparkSession
from pyspark.sql import functions as func
import os

spark = SparkSession.builder.appName("WordCount").getOrCreate()

data_folder = os.getenv('DATA_FOLDER')

# Read each line of my book into a dataframe
inputDF = spark.read.text(data_folder + "/book.txt")

# Split using a regular expression that extracts words
words = inputDF.select(func.explode(func.split(inputDF.value, "\\W+")).alias("word"))
print(words.show())
wordsWithoutEmptyString = words.filter(words.word != "")

# Normalize everything to lowercase
lowercaseWords = wordsWithoutEmptyString.select(func.lower(wordsWithoutEmptyString.word).alias("word"))

# Count up the occurrences of each word
wordCounts = lowercaseWords.groupBy("word").count()

# Sort by counts
wordCountsSorted = wordCounts.sort("count")

# Show the results.
wordCountsSorted.show(wordCountsSorted.count())

spark.stop()
