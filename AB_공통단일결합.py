from pyspark.sql.functions import count, lit
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, col
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, IntegerType
import time
import pandas as pd

#A : id,att1~att10
#B : id,att1~att50

start = time.time()
spark = SparkSession.builder.appName('missing').getOrCreate()
spark.conf.set("spark.sql.analyzer.failAmbiguousSelfJoin", "false")

A_B = spark.read.csv('AB_공통단일결합.csv', header=True, inferSchema=True)
A = spark.read.csv('A_id_attr.csv', header=True, inferSchema=True)
B = spark.read.csv('B_id_attr.csv', header=True, inferSchema=True)

# A_B를 기준으로 A와 조인
result_A = A_B.join(A, A_B.A_id  == A.id, how="inner")
result_A = result_A.withColumnRenamed("A_id", "A_id_a").withColumnRenamed("B_id", "B_id_a").withColumnRenamed("C_id", "C_id_a").withColumnRenamed("id", "id_a")
for i in range(1, 11):
    old_col_name = "attr{}".format(i)
    new_col_name = "{}_a".format(old_col_name)
    result_A = result_A.withColumnRenamed(old_col_name, new_col_name)


# A_B를 기준으로 B와 조인
result_B = A_B.join(B, A_B.B_id == B.id, how="inner")
result_B = result_B.withColumnRenamed("A_id", "A_id_b").withColumnRenamed("B_id", "B_id_b").withColumnRenamed("C_id", "C_id_b").withColumnRenamed("id", "id_b")

# A, B, C의 조인 결과를 A_B를 기준으로 조인
final_result = result_A.join(result_B, result_A . B_id_a == result_B . id_b , how="inner")

# StringType의 열을 포함하여 categorical_columns 정의
categorical_columns = ['attr2_a', 'attr5_a', 'attr10_a', 'attr29', 'attr30', 'attr31', 'attr32', 'attr33', 'attr34', 'attr35', 'attr44', 'attr45']

# 범주형 데이터만을 포함하는 DataFrame 생성
final_result_categorical = final_result.select(*categorical_columns)

# 전체 범주형 데이터 칼럼에 대한 빈도 수와 구성 비 계산
total_summary_df = final_result_categorical.agg(
    count(lit(1)).alias("총 빈도수"),
    (count(lit(1)) / final_result_categorical.count()).alias("총 구성 비")
)
# # 결과 출력
# print("전체 빈도 수와 구성 비:")
# total_summary_df.show(truncate=False)

# 각 범주형 데이터 칼럼에 대한 빈도 수와 구성 비 계산
individual_summaries = []

for col_name in categorical_columns:
    col_summary = final_result_categorical.groupBy(col_name).agg(
        count(lit(1)).alias("빈도수"),
        ((count(lit(1)) / final_result_categorical.count()) * 100).alias("구성 비")
    )
    
    # format_number를 사용하여 소수점 둘째자리까지 표현
    col_summary = col_summary.withColumn("구성 비", format_number(col("구성 비"), 2))
    
    individual_summaries.append((col_name, col_summary))

print("\n각 범주형 데이터 칼럼에 대한 빈도 수와 구성 비:")
for col_name, col_summary in individual_summaries:
    print(f"\n칼럼: {col_name}")
    col_summary.show(truncate=False, vertical=True)




# 결과 확인
# final_result.show(truncate=False)

# column_count = len(final_result.columns)
# print(f"Row count: {final_result.count()}")
# print(f"Column count: {column_count}")

# end = time.time()
# print(end-start)

