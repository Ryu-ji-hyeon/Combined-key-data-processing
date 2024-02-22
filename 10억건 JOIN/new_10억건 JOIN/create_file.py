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
    one = one.withColumn("seq_id", monotonically_increasing_id())
    
    two = spark.read.csv("/home/data/8억건.csv", header=True, inferSchema=True, encoding='utf-8')
    two = two.withColumn("id", (rand() * 800000000).cast("long"))
    two = two.withColumnRenamed("key_1210", "key_1210_two").withColumnRenamed("age_val", "age_val_two").withColumnRenamed("sex_ctgo_cd", "sex_ctgo_cd_two").withColumnRenamed("cstmr_hom_scls_code", "cstmr_hom_scls_code_two")\
             .withColumnRenamed("job_cd", "job_cd_two").withColumnRenamed("indv_incm_amt", "indv_incm_amt_two").withColumnRenamed("seg_34", "seg_34_two").withColumnRenamed("sale_date", "sale_date_two") \
             .withColumnRenamed("auth_time", "auth_time_two").withColumnRenamed("mer_tpbuz_no", "mer_tpbuz_no_two").withColumnRenamed("mer_rgn_scls_code", "mer_rgn_scls_code_two").withColumnRenamed("sale_amt", "sale_amt_two") \
             .withColumnRenamed("column_6", "column_6_two").withColumnRenamed("column_8", "column_8_two").withColumnRenamed("column_9", "column_9_two").withColumnRenamed("column_10", "column_10_two") \
   #  a1_2000 = one.union(two)
   #  a1_2000 = a1_2000.withColumn("seq_id", monotonically_increasing_id())

   #  aaaaa1_2000 = one.union(two)
   #  aaaaa1_2000 = aaaaa1_2000.withColumn("id", (rand() * 800000000).cast("long"))
    
    
    # Create aliases for the DataFrames
    a1_2000_alias = one.alias("a1")
    aaaaa1_2000_alias = two.alias("aaaaa1")    

    # Perform the inner join with aliases
    result = a1_2000_alias.join(aaaaa1_2000_alias, a1_2000_alias.seq_id == aaaaa1_2000_alias.id, how='inner')
  
    end = time.time()
    print("JOIN 성공,",f"경과 시간: {int(end - start)} 초")
    # result.show()
    
    end = time.time()
    

    result.cache()

    # result_id_values = result.select(col("seq_id")).orderBy(desc(col('seq_id'))).limit(10)
    
    end = time.time()
    print("필터링 성공,",f"경과 시간: {int(end - start)} 초")

    # Convert DataFrame to RDD
    result_rdd = result.rdd

    if result_rdd is not None:
       combined_df = rdd(result_rdd)
    
    combined_df.coalesce(1).write.option("header", "true").csv("/home/data/enenen.csv")
    
    end = time.time()
    print("파일 생성 성공,",f"경과 시간: {int(end - start)} 초")
    end = time.time()
    print(f"총 소요 시간: {int(end - start)} 초")







    