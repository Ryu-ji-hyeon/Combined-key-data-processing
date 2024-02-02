from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def rdd_iterate(rdd, chunk_size=1000000):

    print("성공3")
    indexed_rows = rdd.zipWithIndex().cache()
    print("성공4")
    
    # countApprox 메서드의 반환값을 그대로 사용
    count = indexed_rows.countApprox(1000)
    
    print("성공5")
    print("Will iterate through RDD of count {}".format(count))
    
    start = 0
    end = start + chunk_size
    
    while start < count:
        print("성공")
        print("Grabbing new chunk: start = {}, end = {}".format(start, end))
        chunk = indexed_rows.filter(lambda r: r[1] >= start and r[1] < end).collect()
        
        # DataFrame이 아닌 경우에 대한 체크 추가
        for row in chunk:
            if isinstance(row[0], DataFrame):
                yield row[0]
            else:
                # Row 객체를 Spark DataFrame으로 변환
                spark = SparkSession.builder.getOrCreate()
                df = spark.createDataFrame([row[0]])
                yield df
        
        start = end
        end = start + chunk_size

# rdd_iterate의 반환값을 명시적으로 지정
def rdd(rdd):
    return rdd_iterate(rdd)

