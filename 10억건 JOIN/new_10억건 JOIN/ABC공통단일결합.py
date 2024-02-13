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
    a1_2000 = spark.read.csv("/home/data/8억건.csv", header=True, inferSchema=True, encoding='utf-8')
    a1_2000 = a1_2000.withColumn("seq_id", monotonically_increasing_id())
    
    aaaaa1_2000 = spark.read.csv("/home/data/8억건.csv", header=True, inferSchema=True, encoding='utf-8')
    aaaaa1_2000 = aaaaa1_2000.withColumn("id", (rand() * 800000000).cast("long"))
    
    key = spark.read.csv("/home/data/2억건.csv/2억건.csv", header=True, inferSchema=True, encoding='utf-8')
    
    # Create aliases for the DataFrames
    a1_2000_alias = a1_2000.alias("a1")
    aaaaa1_2000_alias = aaaaa1_2000.alias("aaaaa1")
    key_alias = key.alias("key")
    

    # Perform the inner join with aliases
    result1 = a1_2000_alias.join(key_alias, a1_2000_alias.seq_id == key_alias.colNo, how='inner')
    result1 = result1.withColumnRenamed("colNo", "colNo_1")
    
    result2 = aaaaa1_2000_alias.join(key_alias, aaaaa1_2000_alias.id == key_alias.colNo, how='inner')
    result2 = result2.withColumnRenamed("colNo", "colNo_2")
    
    result = result1.join(result2, result1.colNo_1 == result2.colNo_2, how='inner')
        
    
    end = time.time()
    print("JOIN 성공,",f"경과 시간: {int(end - start)} 초")
    # result.show()

    # Cache the DataFrame
    result.cache()

    # Select and show the "id" column values
    result_id_values = result.select(col("colNo_2")).orderBy(desc(col('colNo_2'))).limit(10)
    
    end = time.time()
    print("필터링 성공,",f"경과 시간: {int(end - start)} 초")

    # Convert DataFrame to RDD
    result_rdd = result_id_values.rdd

    if result_rdd is not None:
       combined_df = rdd(result_rdd)
    
    combined_df.show()
    end = time.time()
    print(f"총 소요 시간: {int(end - start)} 초")







    