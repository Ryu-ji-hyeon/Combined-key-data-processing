from pyspark.sql.functions import count, lit
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, col
from pyspark.sql.functions import *
from pyspark.sql.functions import col, max as spark_max
from pyspark.sql.types import StringType, IntegerType
import time
import pandas as pd

#A : id,att1~att10
#B : id,att1~att50

spark = SparkSession.builder.appName('missing').config("spark.executor.memory", "4g").getOrCreate()

spark.conf.set("spark.sql.analyzer.failAmbiguousSelfJoin", "false")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

from pyspark.sql.functions import min

def numeric_data(df, numeric_columns):
    # numeric_columns에 대한 결과만 선택
    numeric_summary = df.select(*numeric_columns)
    quantiles = []

    for col_name in numeric_columns:
        # 정수 타입의 컬럼에 대해서는 Double 형식으로 변환
        numeric_summary = numeric_summary.withColumn(col_name, numeric_summary[col_name].cast('float'))

        min_value = numeric_summary.select(min(col_name)).collect()[0][0]
        quantiles1 = numeric_summary.approxQuantile(col_name, [0.25], 0.01)
        quantiles2 = numeric_summary.approxQuantile(col_name, [0.5], 0.01)
        quantiles3 = numeric_summary.approxQuantile(col_name, [0.75], 0.01)
        quantiles4 = numeric_summary.approxQuantile(col_name, [1.0], 0.01)

        # 수정: 각 값을 한 번에 추가
        quantiles.append([col_name, min_value, quantiles1, quantiles2, quantiles3, quantiles4])
        print(quantiles)

    return quantiles




def categorical_data(df, categorical_columns):
    # 범주형 데이터만을 포함하는 DataFrame 생성
    final_result_categorical = df.select(*categorical_columns)

    # 전체 범주형 데이터 칼럼에 대한 빈도 수와 구성 비 계산
    total_summary_df = final_result_categorical.agg(
        count(lit(1)).alias("총 빈도수"),
        (count(lit(1)) / final_result_categorical.count()).alias("총 구성 비")
    )

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

    # # 결과 출력
    # print("\n각 범주형 데이터 칼럼에 대한 빈도 수와 구성 비:")
    # for col_name, col_summary in individual_summaries:
    #    print(f"\n칼럼: {col_name}")
    #    col_summary.show(truncate=False, vertical=True)
    

    return individual_summaries


def save_to_csv(individual_summaries):

    # 각 범주형 데이터 칼럼에 대한 빈도 수와 구성 비를 저장할 데이터프레임 초기화
    all_data_df = pd.DataFrame()
    output_path = "범주형 데이터.csv"

    # 모든 데이터를 한 개의 CSV 파일로 저장
    for col_name, col_summary in individual_summaries:
        col_summary_pd = pd.DataFrame(col_summary.collect())
        col_summary_pd.columns = [col_name,'빈도수','구성비(%)']
        
        # col_name을 파일 이름으로 사용하여 데이터프레임을 통합
        all_data_df = pd.concat([all_data_df, col_summary_pd], axis=1)


    # 모든 데이터프레임을 한 개의 CSV 파일로 저장
    all_data_df.to_csv(output_path, index=False, encoding='euc-kr')



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


    # 수치형 데이터 추출
    quantiles = numeric_data(final_result, numeric_columns)



    # print(categorical_columns)
    # # 범주형 데이터 추출
    # individual_summaries=categorical_data(final_result, categorical_columns)

    # # 범주형 데이터 CSV 파일로 저장
    # save_to_csv(individual_summaries)

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
