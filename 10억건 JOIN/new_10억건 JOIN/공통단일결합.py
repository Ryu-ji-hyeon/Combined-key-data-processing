from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time
from aaaa import rdd
spark = SparkSession.builder.appName('example').config("spark.local.dir", "/home/spark-temp").getOrCreate()
spark.stop()
spark = SparkSession.builder.appName('example').config("spark.local.dir", "/home/spark-temp").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "10000")


if __name__ == "__main__":
    start = time.time()

    # 데이터 프레임 생성
    one = spark.read.csv("/home/data/8억건.csv", header=True, inferSchema=True, encoding='utf-8')
    one = one.withColumn("id1", monotonically_increasing_id()).withColumn("id2", monotonically_increasing_id()).withColumn("id3", monotonically_increasing_id()).withColumn("id4", monotonically_increasing_id()).withColumn("id5", monotonically_increasing_id()).withColumn("id6", monotonically_increasing_id()).withColumn("id7", monotonically_increasing_id())
    two = spark.read.csv("/home/data/2억건.csv/2억건.csv", header=True, inferSchema=True, encoding='utf-8')
    
    a1_2000 = one.union(two)
    a1_2000 = a1_2000.withColumn("seq_id", monotonically_increasing_id())

    aaaaa1_2000 = one.union(two)
    aaaaa1_2000 = aaaaa1_2000.withColumn("id", (rand() * 800000000).cast("long"))
    
    
    # Create aliases for the DataFrames
    a1_2000_alias = a1_2000.alias("a1")
    aaaaa1_2000_alias = aaaaa1_2000.alias("aaaaa1")    

    # Perform the inner join with aliases
    result = a1_2000_alias.join(aaaaa1_2000_alias, a1_2000_alias.seq_id == aaaaa1_2000_alias.id, how='inner')
  
    end = time.time()
    print("JOIN 성공,",f"경과 시간: {int(end - start)} 초")
    # result.show()

    result.cache()

    result_id_values = result.select(col("seq_id")).orderBy(desc(col('seq_id'))).limit(10)
    
    end = time.time()
    print("필터링 성공,",f"경과 시간: {int(end - start)} 초")

    # Convert DataFrame to RDD
    result_rdd = result_id_values.rdd

    if result_rdd is not None:
       combined_df = rdd(result_rdd)
    
    combined_df.show()
    end = time.time()
    print(f"총 소요 시간: {int(end - start)} 초")







    