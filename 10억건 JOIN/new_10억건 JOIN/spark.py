from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time
from aaaa import rdd
spark = SparkSession.builder.appName('example').config("spark.local.dir", "/home/spark-temp").getOrCreate()
spark.stop()
spark = SparkSession.builder.appName('example').config("spark.local.dir", "/home/spark-temp").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "10000")


if __name__ == "__main__":
    start = time.time()

    # 데이터 프레임 생성
    a1_2000 = spark.read.csv("/home/data/2억건.csv/2억건.csv", header=True, inferSchema=True, encoding='utf-8')
    aaaaa1_2000 = spark.read.csv("/home/data/2억건.csv/2억건.csv", header=True, inferSchema=True, encoding='utf-8')

    # Create aliases for the DataFrames
    a1_2000_alias = a1_2000.alias("a1")
    aaaaa1_2000_alias = aaaaa1_2000.alias("aaaaa1")

    # Perform the inner join with aliases
    result = a1_2000_alias.join(aaaaa1_2000_alias, a1_2000_alias.id == aaaaa1_2000_alias.colNo, how='inner')
    print("성공1")

    # Cache the DataFrame
    result.cache()

    # Select and show the "id" column values
    result_id_values = result.select(col("a1.id")).limit(1)
    print("성공2")

    # Convert DataFrame to RDD
    result_rdd = result_id_values.rdd

    if result_rdd is not None:
        for row in rdd(result_rdd):
            print("성공!!!!!!!!!!!!!!!")








    