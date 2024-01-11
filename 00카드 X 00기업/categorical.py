from pyspark.sql.functions import count, lit, format_number
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import pandas as pd

spark = SparkSession.builder.appName("example").getOrCreate()

def categorical_data(df, categorical_columns):
    # 범주형 데이터만을 포함하는 DataFrame 생성
    final_result_categorical = df.select(*categorical_columns)

    output_path = "범주형 데이터.csv"
    all_data_df = None

    for col_name in categorical_columns:
        col_summary = final_result_categorical.groupBy(col_name).agg(
            count(lit(1)).alias("빈도수"),
            ((count(lit(1)) / final_result_categorical.count()) * 100).alias("구성 비")
        )

        # format_number를 사용하여 소수점 둘째자리까지 표현
        col_summary = col_summary.withColumn("구성 비", format_number(col("구성 비"), 2))
    
        # 모든 데이터프레임을 한 개의 CSV 파일로 저장
        col_summary.write.option("header", "true").csv(f"{output_path}_{col_name}")
    # all_data_df.show()
    