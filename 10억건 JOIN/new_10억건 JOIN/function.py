from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time

    
def rdd_iterate(rdd, chunk_size=1000000):
    print(1)
    indexed_rows = rdd.zipWithIndex().cache()
    count = indexed_rows.countApprox(1000).getFinalValue().mean()
    print("Will iterate through RDD of count {}".format(count))
    start = 0
    end = start + chunk_size
    while start < count:
        print("Grabbing new chunk: start = {}, end = {}".format(start, end))
        chunk = indexed_rows.filter(lambda r: r[1] >= start and r[1] < end).collect()
        for row in chunk:
            yield row[0]
        start = end
        end = start + chunk_size

def rdd_to_csv(rdd):
    for row in rdd_iterate(rdd):
        print(row.show())

