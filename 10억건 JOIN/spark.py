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
    # 2천만건
    a_2000 = spark.read.csv("data/2천만건_컬럼 21.csv", header=True, inferSchema=True,encoding='utf-8')   
    # 1억건
    a_1 = spark.read.csv(("data/plus_id/1억건_id.csv"), header=True, inferSchema=True,encoding='utf-8')

    result_df = pd.DataFrame(join_result)
    result_spark_df = spark.createDataFrame(result_df)

    # result_spark_df에 있는 "id"값들을 가진 행만 필터링
    filtered_result = a_1.filter(col("id").isin(result_spark_df.select("id").rdd.flatMap(lambda x: x).collect()))
    filtered_result.show()
   
    


    row_count = len(filtered_result)
    column_count = filtered_result.shape[1]
    print(f"Row count: {row_count}")
    print(f"Column count: {column_count}")
    end = time.time()
    print(int(end-start),'초')


if __name__ == "__main__":
    
    join_result = data_join()
    data_analyze(join_result)







 