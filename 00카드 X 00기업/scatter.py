from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time
import pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql.types import *

spark = SparkSession.builder.appName('example').config("spark.local.dir", "/home/spark-temp").getOrCreate()
spark.stop()
spark = SparkSession.builder.appName('example').config("spark.local.dir", "/home/spark-temp").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "10000")

aa = spark.read.csv("C:/Users/datarse/Desktop/데이터 처리/00카드 X 00기업/col22,col23.csv", header=True, inferSchema=True, encoding='utf-8')

# new_col13과 col6 간의 상관 계수 계산
correlation_coefficient = aa.corr("col22", "col23")

# 상관 계수 출력
print(correlation_coefficient)

x = aa.select("col22").collect()
y = aa.select("col23").collect()

# 산점도 그리기
plt.scatter(x, y)
plt.xlabel("col22")
plt.ylabel("col23")
plt.show()


