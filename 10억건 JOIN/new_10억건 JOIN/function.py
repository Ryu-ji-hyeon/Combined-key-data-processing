from pyspark.sql import SparkSession
import time
from pyspark.sql.functions import *


def rdd(rdd, chunk_size=1000000):

    indexed_rows = rdd.zipWithIndex().cache()
    
    # countApprox 메서드의 반환값을 그대로 사용
    count = indexed_rows.countApprox(1000)
    print("Will iterate through RDD of count {}".format(count))
    
    start = 0
    end = start + chunk_size
    
    while start < count:
        
        print("Grabbing new chunk: start = {}, end = {}".format(start, end))
        chunk = indexed_rows.filter(lambda r: r[1] >= start and r[1] < end).collect()
        
        # DataFrame이 아닌 경우에 대한 체크 추가
        combined_df = None

        for row in chunk:
            if isinstance(row[0], DataFrame):
                if combined_df is None:
                    combined_df = row[0]
                else:
                    combined_df = combined_df.union(row[0])
            else:
                # Row 객체를 Spark DataFrame으로 변환
                spark = SparkSession.builder.getOrCreate()
                df = spark.createDataFrame([row[0]])

                if combined_df is None:
                    combined_df = df
                else:
                    combined_df = combined_df.union(df)
            
            start = end
            end = start + chunk_size
    return combined_df

    

