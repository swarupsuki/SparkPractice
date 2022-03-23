from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import  col,explode,input_file_name

if __name__ == '__main__':
    sparkConf = SparkConf()
    sc = SparkContext(conf=sparkConf)
    spark = SparkSession.builder.appName("read & write url data").master("local[*]").getOrCreate()

    df = spark.read.format("csv").option("header","true").load("file:///home/saif/LFS/datasets/Multi/Data*/file*.csv")
    # df.show(truncate= False)

    # Method1 uisng list of file path
    print("Method 1")
    df1 = spark.read.format("csv").option("header","true").load(["file:///home/saif/LFS/datasets/Multi/Data1/*.csv","file:///home/saif/LFS/datasets/Multi/Data2/*.csv","file:///home/saif/LFS/datasets/Multi/Data3/*.csv","file:///home/saif/LFS/datasets/Multi/Data4/*.csv"])
    df1.show(truncate= False)
    print("Method 1 Part2 ")
    List_path= ["file:///home/saif/LFS/datasets/Multi/Data1/*.csv","file:///home/saif/LFS/datasets/Multi/Data2/*.csv","file:///home/saif/LFS/datasets/Multi/Data3/*.csv","file:///home/saif/LFS/datasets/Multi/Data4/*.csv"]
    df4 = spark.read.format("csv").option("header", "true").load(List_path)
    df4.show(truncate=False)

    # Method2 using reguler expression [1-3]
    df2 = spark.read.format("csv").option("header","true").load("file:///home/saif/LFS/datasets/Multi/Data[1-4]/*.csv")
    # df2.show(truncate= False)

    # Method3 using reguler expression [1-3]
    df3 = spark.read.format("csv").option("header","true").load("file:///home/saif/LFS/datasets/Multi/Data{1,2,3,4}*/*.csv")
    # df3.show(truncate= False)

    #Method 4 Recurcivefile lookup
    df5 = spark.read.format("csv").option("header", "true").option("recursiveFileLookup","true").load(
        "file:///home/saif/LFS/datasets/Multi/").withColumn("Path",input_file_name()) 
         # Funciotn Input_file_name () is used ot get from whci path record is read
    df5.show(truncate=False)
