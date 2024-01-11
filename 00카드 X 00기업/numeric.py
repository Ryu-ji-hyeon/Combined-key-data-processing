from pyspark.sql import SparkSession
from pyspark.sql.functions import min, lit, count
from pyspark.sql import functions as F
from pyspark.sql.functions import *
import pandas as pd
from pyspark.sql.types import DoubleType,StructField

spark = SparkSession.builder.appName("example").getOrCreate()

def numeric_data(df, numeric_columns):
    numeric_summary = df.select(*numeric_columns)
    quantiles = []

    # 정의된 컬럼
    columns = ['컬럼명', '최소값', '1분위수(25%)', '2분위수(중앙값)', '3분위수(75%)', '최대값']

    for col_name in numeric_columns:
        # 정수 타입의 컬럼에 대해서는 Double 형식으로 변환
        numeric_summary = numeric_summary.withColumn(col_name, numeric_summary[col_name].cast('double'))

        min_value = numeric_summary.select(min(col_name)).collect()[0][0]
        quantiles1 = numeric_summary.approxQuantile(col_name, [0.25], 0.01)
        quantiles2 = numeric_summary.approxQuantile(col_name, [0.5], 0.01)
        quantiles3 = numeric_summary.approxQuantile(col_name, [0.75], 0.01)
        quantiles4 = numeric_summary.approxQuantile(col_name, [1.0], 0.01)

        # 수정: 각 값을 한 번에 추가
        quantiles.append([col_name, float(min_value), float(quantiles1[0]), float(quantiles2[0]), float(quantiles3[0]), float(quantiles4[0])])

    # Spark DataFrame으로 변환
    schema = StructType([StructField(name, DoubleType(), True) for name in columns])
    spark_df = spark.createDataFrame(quantiles, schema=schema)

    # Spark DataFrame 사용
    spark_df.show()