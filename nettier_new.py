from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from numeric_analysis import numeric_data
from categorical_analysis import categorical_data
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import time
import pandas as pd

spark = SparkSession.builder.appName('missing').config("spark.executor.memory", "4g").getOrCreate()

def main():

    start = time.time()
    nettier = spark.read.csv('/home/data/nettier.csv', header=True, inferSchema=True)
    
    # 헤더 변경: 공백 및 특수 문자를 언더스코어로 대체
    new_header = [col.replace(" ", "_").replace("/", "_") for col in nettier.columns]
    nettier = nettier.toDF(*new_header)

    # 첫 번째 행을 추출하여 맨 마지막 행에 추가
    first_row = nettier.head()
    nettier = nettier.union(spark.createDataFrame([first_row], schema=StructType([StructField(name, StringType(), True) for name in new_header])))
    
    nettier = nettier.withColumnRenamed('21', 'colNo')
    nettier = nettier.withColumnRenamed('86d92c35b044193cd530c764f4aee72b3c8fbbeb15a47fc2a080083433a5cf42', 'col1')
    nettier = nettier.withColumnRenamed('25', 'col2')
    nettier = nettier.withColumnRenamed('1', 'col3')
    nettier = nettier.withColumnRenamed('1135', 'col4')
    nettier = nettier.withColumnRenamed('3', 'col5')
    nettier = nettier.withColumnRenamed('3000', 'col6')
    nettier = nettier.withColumnRenamed('_c7', 'col7')
    nettier = nettier.withColumnRenamed('20221125', 'col8')
    nettier = nettier.withColumnRenamed('16', 'col9')
    nettier = nettier.withColumnRenamed('8006', 'col10')
    nettier = nettier.withColumnRenamed('41480550', 'col11')
    nettier = nettier.withColumnRenamed('2000', 'col12')
    nettier = nettier.withColumnRenamed('무료', 'col13')
    nettier = nettier.withColumnRenamed('_c14', 'col14')
    nettier = nettier.withColumnRenamed('_c15', 'col15')
    nettier = nettier.withColumnRenamed('_c16', 'col16')
    
    nettier.show()

    # CSV 형식으로 저장 (하나의 파일로)
    nettier.coalesce(1).write.option("header", "true").csv("/home/data/nettier_new.csv")


    #시간 확인
    end = time.time()
    print(f"경과 시간: {int(end - start)} 초")

if __name__ == "__main__":
    main()
