from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession


if __name__ == '__main__':
    sparkConf = SparkConf()
    sc = SparkContext(conf=sparkConf)
    spark = SparkSession.builder.appName("read Write Mysql").master("local[*]")\
        .getOrCreate()
    # .config("spark.jars","file:///home/saif/LFS/jars/mysql-connector-java-8.0.15.jar")\
    # config  need to be added if the mysql connector jar is not present at cd $SPARK_HOME (/usr/local/hadoop-env/spark-3.0.1/jars) path
    # Here we read the data from db table to dataFrame using spark read
    orderDF = spark.read.format("jdbc")\
        .option("url","jdbc:mysql://localhost:3306/retail_db?useSSL=false") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "orders") \
        .option("user", "root") \
        .option("password", "Welcome@123")\
        .load()

    orderDF.show(truncate=False)
    orderDF.createOrReplaceTempView("orders")
    spark.sql("""Select * from orders where order_status='CLOSED'""").show(truncate=False)

    # there is no need to create table in mysql just provide name under the option dbtable name
    # its create table with same name and write the data in to same table
    orderDF.write.format("jdbc") \
        .mode("overwrite")\
        .option("url", "jdbc:mysql://localhost:3306/retail_db?useSSL=false") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "B7_orders") \
        .option("user", "root") \
        .option("password", "Welcome@123") \
        .save()
    print("Data written successfully")
