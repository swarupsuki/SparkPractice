from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
import urllib.request,json
from urllib.request import urlopen
from pyspark.sql.functions import  col,explode

if __name__ == '__main__':
    # initiated SparkSession and SparkContext 
    sparkConf = SparkConf()
    sc = SparkContext(conf=sparkConf)
    spark = SparkSession.builder.appName("read & write url data").master("local[*]").getOrCreate()


    # Currupt record will come without the decode format
    # readURL = urlopen("https://randomuser.me/api/0.8").read()
    # readURL = urlopen("https://randomuser.me/api/0.8").read().decode("utf-8")
    
    # Read URL data with decode format as utf-8
    readURL = urlopen("https://randomuser.me/api/0.8/?results=10").read().decode("utf-8")
    urlDF= spark.read.json(sc.parallelize([readURL]),multiLine=True) #Multiline option is true for reading multi line jason file 
    urlDF.show(truncate=False)
    urlDF.printSchema()

    urlDF.withColumn("results", explode("results")).show()
    explodeDF = urlDF.withColumn("results",explode("results"))\
        .select("nationality","results.user.cell","results.user.location.city","results.user.name.first",
                "results.user.name.last","results.user.email","results.user.md5","results.user.location.zip")

    explodeDF.show(5, truncate=False)
    explodeDF.printSchema()


    # explodeDF1=urlDF.withColumn("results",explode("results"))
    # explodeDF1.show(truncate=False)
    # explodeDF1.printSchema()

# # Extracting the zip column and insert data into database
    zipDF = explodeDF.select("zip")
    zipDF.show(5,truncate=False)

    for i in range(1,11):
        zipDF.write.format("jdbc")\
                .mode("overwrite")\
                .option("url","jdbc:mysql://localhost:3306/retail_db?useSSL=false") \  # inserted into Mysql db in table B7Zip with overwrite 
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .option("dbtable", "B7Zip") \   # Table Name B7Zip
                .option("user", "root") \
                .option("password", "Welcome@123")\
                .save()
