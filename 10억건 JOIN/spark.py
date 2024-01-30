from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time
from python_ray import data_join
import pandas as pd


spark = SparkSession.builder.appName('missing').config("spark.executor.memory", "32g").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "10000")


def data_analyze(join_result):
    start = time.time()
    # # 8억건
    # aa = spark.read.csv("/home/data/bc_sac_202312141739_final-20231215163224.csv", header=True, inferSchema=True)
    # aa = aa.withColumn("seq_id", monotonically_increasing_id())
    # 2천만건
    a_2000 = spark.read.csv("data/2천만건_컬럼 21.csv", header=True, inferSchema=True,encoding='utf-8')   
    # 1억건
    a_1 = spark.read.csv(("/home/data/2억건.csv/2억건.csv"), header=True, inferSchema=True,encoding='utf-8')

    result_df = pd.DataFrame(join_result)
    result_spark_df = spark.createDataFrame(result_df)

    # result_spark_df에 있는 "id"값들을 가진 행만 필터링
    filtered_result = a_1.filter(col("id").isin(result_spark_df.select("id").rdd.flatMap(lambda x: x).collect()))
    filtered_result.show()

    # column_count = len(filtered_result.columns)
    # print(f"Row count: {filtered_result.count()}")
    # print(f"Column count: {column_count}")
    end = time.time()
    print(int(end-start),'초')


if __name__ == "__main__":
    
    aa = spark.read.csv("/home/data/8억건.csv", header=True, inferSchema=True)
    aa = aa.withColumn("id", monotonically_increasing_id())  
    aa = aa.select(col('id'))
    
    
    join_result = data_join(aa)
    data_analyze(join_result)







 