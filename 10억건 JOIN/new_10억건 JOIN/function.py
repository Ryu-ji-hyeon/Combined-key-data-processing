from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def rdd(rdd, chunk_size=1000000):
    # SparkSession을 가져오거나 새로 생성합니다.
    spark = SparkSession.builder.getOrCreate()

    indexed_rows = rdd.zipWithIndex().cache()
    # countApprox 메서드의 반환값을 그대로 사용
    count = indexed_rows.countApprox(1000)
    print("Will iterate through RDD of count {}".format(count))

    start = 0
    end = start + chunk_size

    combined_df = None

    while start < count:
        print("Grabbing new chunk: start = {}, end = {}".format(start, end))
        chunk = indexed_rows.filter(lambda r: r[1] >= start and r[1] < end).map(lambda x: x[0])

        # 샘플링을 사용하여 스키마를 추론하도록 수정
        chunk_df = spark.createDataFrame(chunk, samplingRatio=0.1)

        if combined_df is None:
            combined_df = chunk_df
        else:
            combined_df = combined_df.union(chunk_df)

        start = end
        end = start + chunk_size

    return combined_df
