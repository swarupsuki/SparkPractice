from pyspark import SparkContext,SparkConf,SparkFiles
from pyspark.sql import SparkSession

if __name__ == '__main__':
    sparkConf = SparkConf()
    sc = SparkContext(conf=sparkConf)
    spark = SparkSession.builder.appName("read & write url data").master("local[*]").getOrCreate()

    readURL = "https://raw.githubusercontent.com/azar-s91/dataset/master/BankChurners.csv"
    spark.sparkContext.addFile(readURL)
    print(SparkFiles.get("BankChurners.csv"))
    urlDF= spark.read.csv("file://"+SparkFiles.get("BankChurners.csv"), header=True,inferSchema=True)
    urlDF.printSchema()
    urlDF.show(5,truncate=False)
