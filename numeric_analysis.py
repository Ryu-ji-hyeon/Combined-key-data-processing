from pyspark.sql import SparkSession
from pyspark.sql.functions import min, lit, count
from pyspark.sql import functions as F
from pyspark.sql.functions import *
import pandas as pd

def numeric_data(df, numeric_columns):
    # numeric_columns에 대한 결과만 선택
    numeric_summary = df.select(*numeric_columns)
    quantiles = []

    for col_name in numeric_columns:
        # 정수 타입의 컬럼에 대해서는 Double 형식으로 변환
        numeric_summary = numeric_summary.withColumn(col_name, numeric_summary[col_name].cast('double'))

        min_value = numeric_summary.select(min(col_name)).collect()[0][0]
        quantiles1 = numeric_summary.approxQuantile(col_name, [0.25], 0.01)
        quantiles2 = numeric_summary.approxQuantile(col_name, [0.5], 0.01)
        quantiles3 = numeric_summary.approxQuantile(col_name, [0.75], 0.01)
        quantiles4 = numeric_summary.approxQuantile(col_name, [1.0], 0.01)

        # 수정: 각 값을 한 번에 추가
        quantiles.append([col_name, min_value, quantiles1, quantiles2, quantiles3, quantiles4])

    # 판다스 데이터프레임으로 변환
    columns = ['col_name', 'min_value', 'quantiles1', 'quantiles2', 'quantiles3', 'quantiles4']
    pd_df = pd.DataFrame(quantiles, columns=columns)

    # CSV 파일로 저장
    output_path = "수치형 데이터.csv"
    pd_df.to_csv(output_path, index=False, encoding='euc-kr') 

    # csv파일 만드는데까지 1시간
    return quantiles