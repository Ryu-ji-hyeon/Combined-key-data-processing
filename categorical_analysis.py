from pyspark.sql.functions import count, lit, format_number
from pyspark.sql import functions as F
from pyspark.sql.functions import *
import pandas as pd

def categorical_data(df, categorical_columns):
    # 범주형 데이터만을 포함하는 DataFrame 생성
    final_result_categorical = df.select(*categorical_columns)

    # 전체 범주형 데이터 칼럼에 대한 빈도 수와 구성 비 계산
    final_result_categorical.agg(
        count(lit(1)).alias("총 빈도수"),
        (count(lit(1)) / final_result_categorical.count()).alias("총 구성 비")
    )

    # 각 범주형 데이터 칼럼에 대한 빈도 수와 구성 비 계산
    individual_summaries = []
    
    # 각 범주형 데이터 칼럼에 대한 빈도 수와 구성 비를 저장할 데이터프레임 초기화
    all_data_df = pd.DataFrame()
    output_path = "범주형 데이터.csv"

    for col_name in categorical_columns:
        col_summary = final_result_categorical.groupBy(col_name).agg(
            count(lit(1)).alias("빈도수"),
            ((count(lit(1)) / final_result_categorical.count()) * 100).alias("구성 비")
        )

        # format_number를 사용하여 소수점 둘째자리까지 표현
        col_summary = col_summary.withColumn("구성 비", format_number(col("구성 비"), 2))

        individual_summaries.append((col_name, col_summary))

        col_summary_pd = pd.DataFrame(col_summary.collect())
        col_summary_pd.columns = [col_name,'빈도수','구성비(%)']
        
        # col_name을 파일 이름으로 사용하여 데이터프레임을 통합
        all_data_df = pd.concat([all_data_df, col_summary_pd], axis=1)


    # 모든 데이터프레임을 한 개의 CSV 파일로 저장
    all_data_df.to_csv(output_path, index=False, encoding='euc-kr')

    # # 결과 출력
    # print("\n각 범주형 데이터 칼럼에 대한 빈도 수와 구성 비:")
    # for col_name, col_summary in individual_summaries:
    #    print(f"\n칼럼: {col_name}")
    #    col_summary.show(truncate=False, vertical=True)
    
    # csv파일 만드는데까지 13분
    return individual_summaries
