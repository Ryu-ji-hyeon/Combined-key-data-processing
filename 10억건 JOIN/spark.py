from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time
from ray import data_join
import pandas as pd


spark = SparkSession.builder.appName('missing').config("spark.executor.memory", "32g").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "10000")


def data_analyze(join_result):
    start = time.time()
    # # 8억건
    # aa = spark.read.csv("/home/data/bc_sac_202312141739_final-20231215163224.csv", header=True, inferSchema=True)
    # 2천만건
    a_2000 = spark.read.csv("/home/data/최종결과_1120_4.csv", header=True, inferSchema=True,encoding='cp949')   
    # 1억건
    a_1 = spark.read.csv("/home/data/plus_id/part-00000-1fadca7f-492d-4d8a-a3ca-de7ac97b188e-c000.csv", header=True, inferSchema=True,encoding='cp949')

    result_df = pd.DataFrame(join_result)
    result_spark_df = spark.createDataFrame(result_df)
    
    result_spark_df.show()
    


    # column_count = len(result_A.columns)
    # print(f"Row count: {a1_2000.count()}")
    # print(f"Column count: {column_count}")
    end = time.time()
    print(int(end-start),'초')


if __name__ == "__main__":
    
    join_result = data_join()
    data_analyze(join_result)







 