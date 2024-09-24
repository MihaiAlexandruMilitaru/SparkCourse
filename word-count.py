from pyspark import SparkConf, SparkContext
import os

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)
data_folder = os.getenv('DATA_FOLDER')

input = sc.textFile(data_folder + "/book.txt")
words = input.flatMap(lambda x: x.split())
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
