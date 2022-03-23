# from pyspark import SparkContext
# from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,explode

if __name__ == '__main__':
    spark = SparkSession.builder.appName("read Write avro file ").master("local[*]")\
        .config("spark.jars","file:///home/saif/LFS/jars/spark-avro_2.12-3.0.1.jar")\  # Jar required for reading the Avro file 
        .getOrCreate()


    avroDF = spark.read.format("avro").load("file:///home/saif/LFS/datasets/users.avro")
    avroDF.printSchema()
    avroDF.show(5, truncate=False)

    resultDF = avroDF.withColumn("Number",explode("favorite_numbers")).select("Name", "favorite_color","Number")
    resultDF.show()

    resultDF.write.format("avro").mode("overwrite").save("hdfs://localhost:9000/user/saif/HFS/Output/Avro")
