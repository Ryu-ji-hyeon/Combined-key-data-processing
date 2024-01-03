from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, col
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, IntegerType
import time

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

# final_result에서 범주형 데이터만 선택
categorical_columns = [col_name for col_name, data_type in final_result.dtypes if data_type == StringType()]

# 범주형 데이터만을 포함하는 DataFrame 생성
final_result_categorical = final_result.select(*categorical_columns)
summary_df = final_result_categorical.groupBy(*categorical_columns).agg(
    count(lit(1)).alias("빈도수"),
    (count(lit(1)) / final_result_categorical.count()).alias("구성 비")
)


# 결과 확인
summary_df.show(truncate=False)

# column_count = len(final_result.columns)
# print(f"Row count: {final_result.count()}")
# print(f"Column count: {column_count}")

# end = time.time()
# print(end-start)

