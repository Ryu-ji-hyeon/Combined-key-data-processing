from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from numeric_analysis import numeric_data
from categorical_analysis import categorical_data
import time
import pandas as pd

spark = SparkSession.builder.appName('missing').config("spark.executor.memory", "4g").getOrCreate()

def main():

    start = time.time()
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

    numeric_columns = [] # 수치형 데이터
    categorical_columns = [] # 범주형 데이터

    # 데이터프레임의 첫번째 행과 마지막 행 추출
    first_row = final_result.head(1)[0]
    last_row = final_result.tail(1)[0]

    for column_name in final_result.columns:
        # 첫번째 행과 마지막 행의 값을 추출
        first_value = first_row[column_name]
        last_value = last_row[column_name]
        
        # 값이 문자열이고, 한자리 수 정수이면 범주형, 나머지 수치형
        if (isinstance(first_value, str) or (first_value >= 0 and first_value < 10)) and \
                (isinstance(last_value, str) or (last_value >= 0 and last_value < 10)) :
            
            if isinstance(first_value, float) or isinstance(last_value, float)  :
               numeric_columns.append(column_name)
            else :
               categorical_columns.append(column_name)
        else:
            numeric_columns.append(column_name)

    exclude_columns = ['A_id_a', 'B_id_a', 'id_a', 'A_id_b', 'B_id_b', 'id_b']

    # # 모든 컬럼에 대해 반복, order by사용
    # max_values = []

    # for column_name in final_result.columns:
    #     # 제외할 컬럼이 아닌 경우에만 진행
    #     if column_name not in exclude_columns:
    #         # 가장 큰 값 찾기
    #         max_value = final_result.agg({column_name: "max"}).collect()[0][0]
    #         max_values.append((column_name, max_value))
    #     # 결과 출력
    # for column, value in max_values:
    #     print(f"\n컬럼: {column}")
    #     print(f"가장 큰 값: {value}")


    # # 수치형 데이터 추출
    # numeric_data(final_result, numeric_columns)

    # # 범주형 데이터 추출
    # categorical_data(final_result, categorical_columns)

    # 조인 결과 확인
    # final_result.show(truncate=False)
    # column_count = len(final_result.columns)
    # print(f"Row count: {final_result.count()}")
    # print(f"Column count: {column_count}")

    #시간 확인
    end = time.time()
    print(f"경과 시간: {int(end - start)} 초")

if __name__ == "__main__":
    main()
