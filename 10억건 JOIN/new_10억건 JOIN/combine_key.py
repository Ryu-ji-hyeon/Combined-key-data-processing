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
    a1 = spark.read.csv("/home/data/2억건.csv/2억건.csv", header=True, inferSchema=True, encoding='utf-8')
   #  a1_2000 = a1_2000.withColumn("seq_id", monotonically_increasing_id())
    a1 = a1.withColumnRenamed("id", "a1_id").withColumnRenamed("colNo", "a1_colNo")
  
    
    a2 = spark.read.csv("/home/data/2억건.csv/2억건.csv", header=True, inferSchema=True, encoding='utf-8')
   #  aaaaa1_2000 = aaaaa1_2000.withColumn("id", (rand() * 800000000).cast("long"))
    a2 = a2.withColumnRenamed("id", "a2_id").withColumnRenamed("colNo", "a2_colNo")
   
    key = spark.read.csv("/home/data/2억건.csv/2억건.csv", header=True, inferSchema=True, encoding='utf-8')
    key = key.withColumnRenamed("id", "key_id").withColumnRenamed("colNo", "key_colNo")

    # Create aliases for the DataFrames
    a1_alias = a1.alias("a1")
    a2_alias = a2.alias("a2")
    key_alias = key.alias("key")

    # Perform the inner join with aliases
    result = a1_alias.join(key_alias, a1_alias.a1_id == key_alias.key_id, how='inner')
    result = result.withColumnRenamed("key_id", "key_id_1")
    
    result1 = a2_alias.join(key_alias, a2_alias.a2_colNo == key_alias.key_colNo, how='inner')
    result1 = result1.withColumnRenamed("key_colNo", "key_colNo_2")
    
    result2 = result.join(result1, result.key_id_1 == result1.key_colNo_2, how='inner')
    end = time.time()
    print("JOIN 성공,",f"경과 시간: {int(end - start)} 초")

    # Cache the DataFrame
    result2.cache()

    # Select and show the "id" column values
    result_id_values = result2.select(col("key_colNo_2")).limit(10)
    end = time.time()
    print("필터링 성공,",f"경과 시간: {int(end - start)} 초")

    # Convert DataFrame to RDD
    result_rdd = result_id_values.rdd

    if result_rdd is not None:
       combined_df = rdd(result_rdd)
    
    combined_df.show()
    end = time.time()
    print("총 소요시간,",f"경과 시간: {int(end - start)} 초")







    