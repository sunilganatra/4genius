import sys
from operator import add

from pyspark.sql import SparkSession


if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("FistSparkSessToConnecctCOS")\
        .getOrCreate()
    sc = spark.sparkContext
    
    hconf = sc._jsc.hadoopConfiguration()
    hconf.set("fs.cos.servicename.endpoint", "s3.us-east.cloud-object-storage.appdomain.cloud")
    # hconf.set("fs.cos.servicename.endpoint", "https://control.cloud-object-storage.cloud.ibm.com/v2/endpoints")

    hconf.set("fs.cos.servicename.access.key","5c5689e7f8c64b7bacdd1c2a39e63672")
    hconf.set("fs.cos.servicename.secret.key","32169ea3fe74231f4b9ad1250ca941b354970ba7b4da21e7")

    df = spark.read.csv("cos://wikipedia-page-views-2015.servicename/", header=True)
    print("Sample data")
    print(df.show(10))
    print("read completed")
    print("total number of rows: ", df.count())
    # print(df.count())
    spark.stop()